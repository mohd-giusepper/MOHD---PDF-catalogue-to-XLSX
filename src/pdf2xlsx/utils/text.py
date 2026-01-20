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
_PRICE_TOKEN_RE = re.compile(
    r"^(?:\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?|\d{1,7}(?:[.,]\d{1,2})?)$"
)
_TOKEN_STRIP_CHARS = " ,;:()[]{}<>|"
_CURRENCY_TOKENS = {
    "EUR",
    "DKK",
    "SEK",
    "NOK",
    "USD",
    "GBP",
    "CHF",
    "JPY",
    "CAD",
    "AUD",
}
_DIMENSION_X_RE = re.compile(
    r"^\d+(?:[.,]\d+)?(?:x\d+(?:[.,]\d+)?){1,2}(?:cm|mm|m)?$",
    re.IGNORECASE,
)
_DIMENSION_LABEL_RE = re.compile(
    r"^(?:H|W|D|L|O)\s*\d+(?:[.,]\d+)?(?:\s*(?:cm|mm|m))?$",
    re.IGNORECASE,
)
_DIMENSION_LABEL_PREFIX_RE = re.compile(
    r"^(?:H|W|D|L|O)\d+(?:[.,]\d+)?(?:cm|mm|m)?$",
    re.IGNORECASE,
)
_X_TOKENS = {"x", "X", "\u00d7"}
_UNIT_TOKENS = {"cm", "mm", "m", "mq", "sqm", "m2"}


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


def tokenize_line(line: str) -> List[str]:
    if not line:
        return []
    normalized = line.replace("\u20ac", " \u20ac ")
    parts = re.split(r"\s+", normalized.strip())
    tokens: List[str] = []
    for part in parts:
        cleaned = part.strip(_TOKEN_STRIP_CHARS)
        if cleaned:
            tokens.append(cleaned)
    return tokens


def classify_token(token: str) -> str:
    if not token:
        return "TEXT"
    if is_dimension_candidate_token(token):
        return "DIM_CANDIDATE"
    if is_currency_token(token):
        return "PRICE_CANDIDATE"
    cleaned = strip_currency_token(token)
    if _PRICE_TOKEN_RE.match(cleaned) and parse_price(cleaned) is not None:
        return "PRICE_CANDIDATE"
    if is_art_no_candidate_token(token):
        return "ARTNO_CANDIDATE"
    return "TEXT"


def resolve_row_fields(line: str) -> Dict[str, object]:
    """Extract candidates and selections; set ambiguous_numeric when numeric art_no vs price is unclear."""
    tokens = tokenize_line(line)
    consumed = set()
    dimension_candidates: List[str] = []

    for idx, token in enumerate(tokens):
        if is_dimension_candidate_token(token):
            dimension_candidates.append(token)
            consumed.add(idx)

    idx = 0
    while idx < len(tokens):
        if idx in consumed:
            idx += 1
            continue
        token = tokens[idx]
        if is_number_token(token) and idx + 2 < len(tokens):
            sep = tokens[idx + 1]
            tail = tokens[idx + 2]
            if sep in _X_TOKENS and is_number_token(tail):
                span_tokens = [token, sep, tail]
                end = idx + 3
                if end + 1 < len(tokens) and tokens[end] in _X_TOKENS and is_number_token(tokens[end + 1]):
                    span_tokens.extend([tokens[end], tokens[end + 1]])
                    end += 2
                if end < len(tokens) and is_unit_token(tokens[end]):
                    span_tokens.append(tokens[end])
                    end += 1
                dimension_candidates.append(" ".join(span_tokens))
                consumed.update(range(idx, end))
                idx = end
                continue
        if is_dimension_label_token(token) and idx + 1 < len(tokens) and is_number_token(tokens[idx + 1]):
            span_tokens = [token, tokens[idx + 1]]
            end = idx + 2
            if end < len(tokens) and is_unit_token(tokens[end]):
                span_tokens.append(tokens[end])
                end += 1
            dimension_candidates.append(" ".join(span_tokens))
            consumed.update(range(idx, end))
            idx = end
            continue
        idx += 1

    currency_indices = {i for i, token in enumerate(tokens) if is_currency_token(token)}
    price_candidates: List[Dict[str, object]] = []
    for idx, token in enumerate(tokens):
        if idx in consumed:
            continue
        cleaned = strip_currency_token(token)
        if not _PRICE_TOKEN_RE.match(cleaned):
            continue
        value = parse_price(cleaned)
        if value is None:
            continue
        has_currency = (
            idx in currency_indices
            or (idx - 1) in currency_indices
            or (idx + 1) in currency_indices
            or token_has_currency(token)
        )
        score = 0
        if has_currency:
            score += 3
        if _THOUSANDS_RE.match(cleaned):
            score += 2
        if _DECIMAL_2_RE.search(cleaned):
            score += 1
        if value >= 100:
            score += 1
        price_candidates.append(
            {
                "token": cleaned,
                "value": value,
                "score": score,
                "idx": idx,
                "has_currency": has_currency,
            }
        )

    art_no_candidates: List[Dict[str, object]] = []
    for idx, token in enumerate(tokens):
        if idx in consumed:
            continue
        if is_currency_token(token):
            continue
        candidate = canonicalize_art_no(token)
        if not candidate:
            continue
        if re.search(r"[A-Za-z]", candidate) and re.search(r"\d", candidate):
            art_no_candidates.append(
                {
                    "token": candidate,
                    "score": 3,
                    "idx": idx,
                    "kind": "alnum",
                }
            )
            continue
        digits_only = re.sub(r"\D", "", candidate)
        numeric_like = candidate.isdigit() or bool(re.fullmatch(r"\d+(?:[-./]\d+)+", candidate))
        if numeric_like:
            if idx in currency_indices or (idx - 1) in currency_indices or (idx + 1) in currency_indices:
                continue
            if _THOUSANDS_RE.match(token) or _DECIMAL_2_RE.search(token):
                continue
            if 3 <= len(digits_only) <= 6:
                score = 2
            elif 7 <= len(digits_only) <= 10:
                score = 1
            else:
                continue
            art_no_candidates.append(
                {
                    "token": candidate,
                    "score": score,
                    "idx": idx,
                    "kind": "numeric",
                }
            )

    price_candidates.sort(key=lambda item: (item["score"], item["value"]), reverse=True)
    art_no_candidates.sort(key=lambda item: (item["score"], len(item["token"])), reverse=True)

    selected_price = price_candidates[0] if price_candidates else None
    selected_art_no = art_no_candidates[0] if art_no_candidates else None

    ambiguous_numeric = False
    if selected_art_no and selected_art_no.get("kind") == "numeric":
        numeric_tokens = {
            item["token"] for item in art_no_candidates if item.get("kind") == "numeric"
        }
        price_tokens = {item["token"] for item in price_candidates}
        overlap = numeric_tokens & price_tokens
        if overlap:
            strong_alt_price = any(
                item["score"] >= 3 and item["token"] not in overlap
                for item in price_candidates
            )
            if not strong_alt_price:
                ambiguous_numeric = True

    consumed_name_indices = set(consumed)
    if selected_price:
        consumed_name_indices.add(int(selected_price["idx"]))
        for adj in (int(selected_price["idx"]) - 1, int(selected_price["idx"]) + 1):
            if 0 <= adj < len(tokens) and is_currency_token(tokens[adj]):
                consumed_name_indices.add(adj)
    if selected_art_no:
        consumed_name_indices.add(int(selected_art_no["idx"]))

    name_tokens = [
        token
        for idx, token in enumerate(tokens)
        if idx not in consumed_name_indices and not is_currency_token(token)
    ]
    name = " ".join(name_tokens).strip()

    return {
        "tokens": tokens,
        "dimension_candidates": dimension_candidates,
        "price_candidates": price_candidates,
        "art_no_candidates": art_no_candidates,
        "selected_price": selected_price,
        "selected_art_no": selected_art_no,
        "ambiguous_numeric": ambiguous_numeric,
        "name": name,
    }


def is_currency_token(token: str) -> bool:
    if not token:
        return False
    if token == "\u20ac":
        return True
    return token.upper() in _CURRENCY_TOKENS


def token_has_currency(token: str) -> bool:
    if not token:
        return False
    if "\u20ac" in token:
        return True
    upper = token.upper()
    return any(code in upper for code in _CURRENCY_TOKENS)


def strip_currency_token(token: str) -> str:
    if not token:
        return ""
    value = token.replace("\u20ac", "")
    for code in _CURRENCY_TOKENS:
        value = re.sub(rf"^{code}", "", value, flags=re.IGNORECASE)
        value = re.sub(rf"{code}$", "", value, flags=re.IGNORECASE)
    return value.strip()


def is_number_token(token: str) -> bool:
    return parse_number(token) is not None


def is_unit_token(token: str) -> bool:
    return token.lower() in _UNIT_TOKENS


def is_dimension_label_token(token: str) -> bool:
    if not token:
        return False
    return token.upper() in {"H", "W", "D", "L", "O"}


def is_dimension_candidate_token(token: str) -> bool:
    if not token:
        return False
    normalized = token.replace("\u00d7", "x")
    collapsed = normalized.replace(" ", "")
    if _DIMENSION_X_RE.match(collapsed):
        return True
    if _DIMENSION_LABEL_RE.match(normalized):
        return True
    if _DIMENSION_LABEL_PREFIX_RE.match(normalized):
        return True
    return False


def is_art_no_candidate_token(token: str) -> bool:
    if not token:
        return False
    candidate = canonicalize_art_no(token)
    if not candidate:
        return False
    if re.search(r"[A-Za-z]", candidate) and re.search(r"\d", candidate):
        return True
    digits_only = re.sub(r"\D", "", candidate)
    if candidate.isdigit() or re.fullmatch(r"\d+(?:[-./]\d+)+", candidate):
        return 3 <= len(digits_only) <= 10
    return False


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
