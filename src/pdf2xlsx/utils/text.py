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
_UNIT_RE = re.compile(
    r"\b(cm|mm|mq|sqm|m2|m3|kg|h\.|dia|diam|m\u00b2|m\u00b3)\b", re.IGNORECASE
)
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
_YEAR_MIN = 2000
_YEAR_MAX = 2026
_TOC_YEAR_PRICE_RE = re.compile(
    r"\b(?P<num>\d{1,3})\s+(?P<year>19\d{2}|20\d{2})\s+(?:EUR|\u20ac)\b",
    re.IGNORECASE,
)
_YEAR_EUR_RE = re.compile(
    r"\b(?:EUR|\u20ac)\s*(?P<year>19\d{2}|20\d{2})\b"
    r"|\b(?P<year_alt>19\d{2}|20\d{2})\s*(?:EUR|\u20ac)\b",
    re.IGNORECASE,
)
_SHORT_NUMBER_RE = re.compile(r"\b\d{1,3}\b")
_CODE_LABEL_RE = re.compile(
    r"\b(?:code|cod\.?|codice|art\.?\s*no\.?|item|ref\.?|nr\.?)\b",
    re.IGNORECASE,
)
_MEASURE_CONTEXT_RE = re.compile(
    r"\b(cm|mm|mq|sqm|m2|m3|kg|volume|seat\s*h|fabric\s*meters?)\b",
    re.IGNORECASE,
)
_COLOR_TEMP_RE = re.compile(r"^\d{3,4}K\.?$", re.IGNORECASE)
_SHORT_GRADE_TOKEN_RE = re.compile(r"^[A-Za-z]\d{1,2}$")
_SPEC_CONTEXT_RE = re.compile(r"\b(led\s*/\s*m|led/m|dimmable|strip|bulb)\b", re.IGNORECASE)
_WATT_RE = re.compile(r"^\d{1,3}\s*W$", re.IGNORECASE)
_E_SOCKET_RE = re.compile(r"^E\d{2}(?:/E\d{2})?$", re.IGNORECASE)
_T_SERIES_RE = re.compile(r"^T\d{2}(?:/T\d{2})+$", re.IGNORECASE)
_SINGLE_LETTER_RE = re.compile(r"^[A-Za-z]$")
_NUMBER_WITH_UNIT_RE = re.compile(
    r"^\d+(?:[.,]\d+)?(?:cm|mm|m|m2|m3|sqm|mq|kg|m\u00b2|m\u00b3)$",
    re.IGNORECASE,
)
_DIMENSION_X_RE = re.compile(
    r"^\d+(?:[.,]\d+)?(?:x\d+(?:[.,]\d+)?){1,2}(?:cm|mm|m)?$",
    re.IGNORECASE,
)
_DIMENSION_LABEL_RE = re.compile(
    r"^(?:H|W|D|L|O|P)\s*\d+(?:[.,]\d+)?(?:\s*(?:cm|mm|m))?$",
    re.IGNORECASE,
)
_DIMENSION_LABEL_PREFIX_RE = re.compile(
    r"^(?:H|W|D|L|O|P)\d+(?:[.,]\d+)?(?:cm|mm|m)?$",
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
    cleaned = re.sub(r"\s+", "", token).strip()
    if len(cleaned) < min_len:
        return False
    if not is_valid_art_no_token(cleaned, min_len=min_len):
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


def is_blacklisted_art_no_token(token: str) -> bool:
    if not token:
        return False
    candidate = canonicalize_art_no(token)
    if not candidate:
        return False
    if _COLOR_TEMP_RE.match(candidate):
        return True
    if _WATT_RE.match(candidate):
        return True
    if _E_SOCKET_RE.match(candidate):
        return True
    if _T_SERIES_RE.match(candidate):
        return True
    if _SINGLE_LETTER_RE.match(candidate):
        return True
    if _SHORT_GRADE_TOKEN_RE.match(candidate):
        return True
    return False


def is_valid_art_no_token(token: str, min_len: int = 6) -> bool:
    if not token:
        return False
    candidate = canonicalize_art_no(token)
    if not candidate:
        return False
    min_len = max(min_len, 6)
    if len(candidate) < min_len:
        return False
    if is_dimension_token(candidate):
        return False
    if is_date_token(candidate):
        return False
    if _NUMBER_WITH_UNIT_RE.match(candidate):
        return False
    if is_blacklisted_art_no_token(candidate):
        return False
    if not any(char.isalpha() for char in candidate):
        return False
    if not any(char.isdigit() for char in candidate):
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
    code_label_present = has_code_label(line)
    dimension_context = _has_dimension_context(line, tokens)
    unit_context_indices = _collect_unit_context_indices(tokens)
    toc_year_price = is_toc_year_price_line(line) or is_toc_like_line(line)
    spec_context = has_spec_context(line, tokens)
    filtered_color_temp_code = 0
    filtered_short_grade_token = 0
    filtered_watt_code = 0
    filtered_socket_code = 0
    filtered_t_series_code = 0
    filtered_single_letter_code = 0

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
    year_price_blocked = 0
    for idx, token in enumerate(tokens):
        if idx in consumed:
            continue
        if _is_unit_context(tokens, idx, unit_context_indices):
            continue
        cleaned = strip_currency_token(token)
        if not _PRICE_TOKEN_RE.match(cleaned):
            continue
        if is_year_price_candidate(cleaned, token):
            year_price_blocked += 1
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
        if _COLOR_TEMP_RE.match(candidate):
            filtered_color_temp_code += 1
            continue
        if _WATT_RE.match(candidate):
            filtered_watt_code += 1
            continue
        if _E_SOCKET_RE.match(candidate):
            filtered_socket_code += 1
            continue
        if _T_SERIES_RE.match(candidate):
            filtered_t_series_code += 1
            continue
        if _SINGLE_LETTER_RE.match(candidate):
            filtered_single_letter_code += 1
            continue
        if is_short_grade_token(candidate) and not code_label_present:
            filtered_short_grade_token += 1
            continue
        if spec_context and not code_label_present and len(candidate) <= 5:
            continue
        if is_valid_art_no_token(candidate, min_len=6):
            art_no_candidates.append(
                {
                    "token": candidate,
                    "score": 3,
                    "idx": idx,
                    "kind": "alnum",
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
    numeric_art_no = bool(selected_art_no and selected_art_no.get("kind") == "numeric")
    numeric_art_no_dimension = numeric_art_no and dimension_context
    numeric_art_no_strong = False
    if numeric_art_no:
        if code_label_present:
            numeric_art_no_strong = True
        elif selected_price and selected_price.get("has_currency"):
            numeric_art_no_strong = True
    numeric_art_no_ambiguous = numeric_art_no and not numeric_art_no_strong

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
        "numeric_art_no": numeric_art_no,
        "numeric_art_no_dimension": numeric_art_no_dimension,
        "numeric_art_no_strong": numeric_art_no_strong,
        "numeric_art_no_ambiguous": numeric_art_no_ambiguous,
        "toc_year_price": toc_year_price,
        "year_price_blocked": year_price_blocked,
        "filtered_color_temp_code": filtered_color_temp_code,
        "filtered_short_grade_token": filtered_short_grade_token,
        "filtered_watt_code": filtered_watt_code,
        "filtered_socket_code": filtered_socket_code,
        "filtered_t_series_code": filtered_t_series_code,
        "filtered_single_letter_code": filtered_single_letter_code,
        "spec_context": spec_context,
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
    return token.upper() in {"H", "W", "D", "L", "O", "P"}


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
    return is_valid_art_no_token(candidate, min_len=6)


def analyze_line(line: str) -> Dict[str, object]:
    line = line or ""
    has_currency = bool(_CURRENCY_RE.search(line))
    has_dimension = bool(_DIMENSION_IN_LINE_RE.search(line)) or bool(_UNIT_RE.search(line))
    tokens = tokenize_line(line)
    has_measure_context = _has_dimension_context(line, tokens)
    number_tokens = [match.group(0) for match in _NUMBER_TOKEN_RE.finditer(line)]
    candidates = []
    unit_context_indices = _collect_unit_context_indices(tokens)
    year_price_blocked = 0
    for idx, token in enumerate(tokens):
        if _is_unit_context(tokens, idx, unit_context_indices):
            continue
        cleaned = strip_currency_token(token)
        if not _PRICE_TOKEN_RE.match(cleaned):
            continue
        if is_year_price_candidate(cleaned, token):
            year_price_blocked += 1
            continue
        value = parse_number(cleaned)
        if value is None:
            continue
        score = 0
        if has_currency:
            score += 3
        if _THOUSANDS_RE.match(cleaned):
            score += 2
        if _DECIMAL_2_RE.search(cleaned):
            score += 1
        if value >= 100:
            score += 1
        candidates.append(
            {
                "token": cleaned,
                "value": value,
                "score": score,
                "thousands": bool(_THOUSANDS_RE.match(cleaned)),
            }
        )

    best = max((item["score"] for item in candidates), default=0)
    max_value = max((item["value"] for item in candidates), default=0.0)
    number_count = len(number_tokens)
    price_like = False
    if best >= 3:
        price_like = True
    elif best >= 2:
        price_like = max_value >= 10 or has_currency
    elif best >= 1:
        price_like = (not has_dimension) and number_count <= 3 and max_value >= 10

    if has_measure_context and not has_currency:
        price_like = False

    dimension_line = False
    if has_dimension or has_measure_context:
        dimension_line = True
    elif number_count >= 4 and best <= 2:
        dimension_line = True
    elif number_count >= 3 and max_value < 10 and not has_currency:
        dimension_line = True

    return {
        "has_currency": has_currency,
        "has_dimension": has_dimension or has_measure_context,
        "dimension_line": dimension_line,
        "number_count": number_count,
        "price_like": price_like,
        "candidates": candidates,
        "toc_year_price": is_toc_year_price_line(line) or is_toc_like_line(line),
        "year_price_blocked": year_price_blocked,
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
    if is_year_price_candidate(value, value):
        return None
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


def _year_bounds() -> Tuple[int, int]:
    return _YEAR_MIN, _YEAR_MAX


def is_year_price_candidate(cleaned: str, raw_token: str) -> bool:
    if not cleaned:
        return False
    if not cleaned.isdigit():
        return False
    if "," in raw_token or "." in raw_token:
        return False
    try:
        value = int(cleaned)
    except ValueError:
        return False
    year_min, year_max = _year_bounds()
    return year_min <= value <= year_max


def is_toc_year_price_line(line: str, min_pairs: int = 2) -> bool:
    if not line:
        return False
    pairs = extract_toc_year_price_pairs(line)
    if len(pairs) < min_pairs:
        return False
    distinct_nums = {item[0] for item in pairs}
    return len(distinct_nums) >= min_pairs


def extract_toc_year_price_pairs(text: str) -> List[Tuple[int, int]]:
    pairs: List[Tuple[int, int]] = []
    if not text:
        return pairs
    year_min, year_max = _year_bounds()
    for match in _TOC_YEAR_PRICE_RE.finditer(text):
        try:
            num = int(match.group("num"))
            year = int(match.group("year"))
        except ValueError:
            continue
        if year_min <= year <= year_max:
            pairs.append((num, year))
    return pairs


def count_year_eur_hits(text: str) -> int:
    if not text:
        return 0
    year_min, year_max = _year_bounds()
    hits = 0
    for match in _YEAR_EUR_RE.finditer(text):
        year_text = match.group("year") or match.group("year_alt") or ""
        try:
            year = int(year_text)
        except ValueError:
            continue
        if year_min <= year <= year_max:
            hits += 1
    return hits


def count_short_numbers(text: str) -> int:
    if not text:
        return 0
    return len(_SHORT_NUMBER_RE.findall(text))


def count_plausible_code_tokens(text: str) -> int:
    if not text:
        return 0
    tokens = tokenize_line(text)
    return sum(
        1 for token in tokens if is_plausible_code(token, min_len=6)
    )


def is_toc_like_line(line: str) -> bool:
    if not line:
        return False
    if "EUR" not in line.upper() and "\u20ac" not in line:
        return False
    year_hits = count_year_eur_hits(line)
    if year_hits <= 0:
        return False
    short_numbers = count_short_numbers(line)
    if short_numbers < 3:
        return False
    return count_plausible_code_tokens(line) == 0


def has_code_label(line: str) -> bool:
    if not line:
        return False
    return bool(_CODE_LABEL_RE.search(line))


def is_short_grade_token(token: str) -> bool:
    if not token:
        return False
    return bool(_SHORT_GRADE_TOKEN_RE.match(token))


def has_spec_context(line: str, tokens: List[str]) -> bool:
    if not line:
        return False
    if _SPEC_CONTEXT_RE.search(line):
        return True
    for token in tokens:
        if token.lower() in {"led/m", "dimmable", "strip", "bulb"}:
            return True
    return False


def _has_dimension_context(line: str, tokens: List[str]) -> bool:
    if not line:
        return False
    if _DIMENSION_IN_LINE_RE.search(line):
        return True
    if _UNIT_RE.search(line):
        return True
    if _MEASURE_CONTEXT_RE.search(line):
        return True
    if '"' in line:
        return True
    for token in tokens:
        upper = token.upper()
        if upper in {"L", "P", "H", "W", "D", "O", "Ø"}:
            return True
    return False


def _collect_unit_context_indices(tokens: List[str]) -> set:
    indices = set()
    for idx, token in enumerate(tokens):
        lower = token.lower()
        upper = token.upper()
        if _NUMBER_WITH_UNIT_RE.match(token):
            indices.add(idx)
            continue
        if lower in {"cm", "mm", "m", "m2", "m3", "sqm", "mq", "kg", "volume"}:
            indices.add(idx)
            continue
        if upper in {"L", "P", "H", "W", "D", "O", "Ø"}:
            indices.add(idx)
            continue
        if lower in {"seat", "fabric", "meters", "meter"}:
            indices.add(idx)
            continue
        if '"' in token:
            indices.add(idx)
            continue
    return indices


def _is_unit_context(tokens: List[str], idx: int, unit_indices: set) -> bool:
    if idx in unit_indices:
        return True
    for offset in (-2, -1, 1, 2):
        adj = idx + offset
        if adj in unit_indices:
            return True
        if 0 <= adj < len(tokens) and '"' in tokens[adj]:
            return True
    if 0 <= idx < len(tokens) and '"' in tokens[idx]:
        return True
    return False
