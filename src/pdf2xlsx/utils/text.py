import re
from typing import Dict, List, Optional, Tuple


def normalize_decimal(value: str) -> str:
    return value.replace(",", ".")


_DIMENSION_TOKEN_RE = re.compile(r"^\d{2,5}\s*[xX]\s*\d{2,5}$")
_DATE_TOKEN_RE = re.compile(r"^\d{1,2}/\d{4}$")
_DIMENSION_IN_LINE_RE = re.compile(r"\b\d{2,5}\s*[xX]\s*\d{2,5}\b")
_CURRENCY_RE = re.compile(
    r"(?:\u20ac|\bEUR\b|\bDKK\b|\bSEK\b|\bNOK\b|\bUSD\b|\bGBP\b|\bCHF\b|\bJPY\b|\bCAD\b|\bAUD\b)",
    re.IGNORECASE,
)
_UNIT_RE = re.compile(r"\b(cm|mm|mq|sqm|m2|kg|h\.|dia|diam)\b", re.IGNORECASE)
_NUMBER_TOKEN_RE = re.compile(
    r"\b\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?\b|\b\d{1,7}(?:[.,]\d{1,2})?\b"
)
_THOUSANDS_RE = re.compile(r"^\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{2})?$")
_DECIMAL_2_RE = re.compile(r"[.,]\d{2}$")


def is_dimension_token(token: str) -> bool:
    return bool(_DIMENSION_TOKEN_RE.match(token.strip()))


def is_date_token(token: str) -> bool:
    return bool(_DATE_TOKEN_RE.match(token.strip()))


def is_plausible_code(token: str, min_len: int = 5) -> bool:
    if not token:
        return False
    cleaned = re.sub(r"\s+", "", token)
    cleaned = cleaned.strip()
    if len(cleaned) < min_len:
        return False
    if is_dimension_token(cleaned):
        return False
    if is_date_token(cleaned):
        return False
    if not any(char.isalpha() for char in cleaned):
        return False
    if not any(char.isdigit() for char in cleaned):
        return False

    alnum = [char for char in cleaned if char.isalnum()]
    if not alnum:
        return False
    alnum_ratio = len(alnum) / len(cleaned)
    if alnum_ratio < 0.6:
        return False
    digit_ratio = sum(1 for char in alnum if char.isdigit()) / len(alnum)
    if digit_ratio < 0.2 or digit_ratio > 0.85:
        return False

    transitions = 0
    last_kind = None
    for char in alnum:
        kind = "d" if char.isdigit() else "a"
        if last_kind and kind != last_kind:
            transitions += 1
        last_kind = kind
    if len(alnum) >= 5:
        if transitions / max(1, len(alnum) - 1) > 0.7 and len(alnum) <= 8:
            return False
    if re.search(r"(.)\1\1", cleaned):
        return False
    return True


def analyze_line(line: str) -> Dict[str, object]:
    line = line or ""
    has_currency = bool(_CURRENCY_RE.search(line))
    has_dimension = bool(_DIMENSION_IN_LINE_RE.search(line)) or bool(_UNIT_RE.search(line))
    number_tokens = [match.group(0) for match in _NUMBER_TOKEN_RE.finditer(line)]
    candidates = []
    for token in number_tokens:
        value = parse_number(token)
        if value is None:
            continue
        score = 0
        if has_currency:
            score += 3
        if _THOUSANDS_RE.match(token):
            score += 2
        if _DECIMAL_2_RE.search(token):
            score += 1
        if value >= 100:
            score += 1
        candidates.append(
            {
                "token": token,
                "value": value,
                "score": score,
                "thousands": bool(_THOUSANDS_RE.match(token)),
            }
        )

    best = max((item["score"] for item in candidates), default=0)
    max_value = max((item["value"] for item in candidates), default=0.0)
    number_count = len(candidates)
    price_like = False
    if best >= 3:
        price_like = True
    elif best >= 2:
        price_like = max_value >= 10 or has_currency
    elif best >= 1:
        price_like = (not has_dimension) and number_count <= 3 and max_value >= 10

    dimension_line = False
    if has_dimension:
        dimension_line = True
    elif number_count >= 4 and best <= 2:
        dimension_line = True
    elif number_count >= 3 and max_value < 10 and not has_currency:
        dimension_line = True

    return {
        "has_currency": has_currency,
        "has_dimension": has_dimension,
        "dimension_line": dimension_line,
        "number_count": number_count,
        "price_like": price_like,
        "candidates": candidates,
    }


def pick_price_candidate(line: str) -> Tuple[Optional[float], str, int]:
    info = analyze_line(line)
    candidates = info.get("candidates") or []
    if not candidates:
        return None, "", 0
    best_score = max(item["score"] for item in candidates)
    best_items = [item for item in candidates if item["score"] == best_score]
    chosen = best_items[-1]
    return float(chosen["value"]), str(chosen["token"]), int(best_score)


def extract_dimension(size_raw: str, key: str) -> Optional[float]:
    pattern = rf"\b{re.escape(key)}\b\s*([0-9]+(?:[.,][0-9]+)?)\s*cm"
    match = re.search(pattern, size_raw, re.IGNORECASE)
    if not match:
        return None
    value = normalize_decimal(match.group(1))
    try:
        return float(value)
    except ValueError:
        return None


def parse_number(value: str) -> Optional[float]:
    cleaned = re.sub(r"[^0-9,\.]", "", value or "")
    cleaned = cleaned.strip()
    if not cleaned:
        return None

    last_comma = cleaned.rfind(",")
    last_dot = cleaned.rfind(".")

    if last_comma != -1 and last_dot != -1:
        decimal_sep = "," if last_comma > last_dot else "."
        thousands_sep = "." if decimal_sep == "," else ","
        cleaned = cleaned.replace(thousands_sep, "")
        cleaned = cleaned.replace(decimal_sep, ".")
    elif last_comma != -1:
        decimals = len(cleaned) - last_comma - 1
        if decimals == 3 and len(cleaned) > 4:
            cleaned = cleaned.replace(",", "")
        else:
            cleaned = cleaned.replace(",", ".")
    elif last_dot != -1:
        decimals = len(cleaned) - last_dot - 1
        if decimals == 3 and len(cleaned) > 4:
            cleaned = cleaned.replace(".", "")
        else:
            cleaned = cleaned.replace(",", "")

    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_price(value: str, min_value: float = 0.01, max_value: float = 1_000_000) -> Optional[float]:
    number = parse_number(value)
    if number is None:
        return None
    if number <= 0 or number >= max_value:
        return None
    if number < min_value:
        return None
    return number


def canonicalize_art_no(value: str) -> str:
    if not value:
        return ""
    normalized = value.replace("\u00ad", "-")
    normalized = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", normalized)
    normalized = normalized.strip()
    normalized = re.sub(r"\s+", "", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized)
    return normalized


def normalize_art_no(value: str) -> str:
    return canonicalize_art_no(value)
