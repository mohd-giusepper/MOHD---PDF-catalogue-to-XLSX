import re
from typing import Optional


def normalize_decimal(value: str) -> str:
    return value.replace(",", ".")


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
