import re
from typing import Dict, List, Tuple

from pdf2xlsx import config
from pdf2xlsx.utils import text as text_utils


_UNIT_TOKENS = {"cm", "mm", "m", "m2", "m3", "sqm", "mq", "kg", "volume"}


def stitch_page_words(
    words: List[dict],
    target_currency: str,
    max_rows: int,
    y_tolerance: float,
) -> Tuple[List[dict], dict]:
    if not words:
        return [], {"rows_built": 0, "rows_ambiguous": 0}
    rows: List[dict] = []
    rows_built = 0
    rows_ambiguous = 0
    last_code = None
    last_code_idx = None
    used_codes = set()
    clusters = _cluster_by_line(words, y_tolerance)
    for cluster_idx, cluster in enumerate(clusters):
        if rows_built >= max_rows:
            break
        tokens = _tokens_from_cluster(cluster)
        if not tokens:
            continue
        line_text = " ".join(token["text"] for token in tokens)
        code_label_present = text_utils.has_code_label(line_text)
        spec_context = text_utils.has_spec_context(line_text, [t["text"] for t in tokens])
        codes = _code_candidates(tokens, code_label_present, spec_context)
        prices = _price_candidates(tokens)
        if codes and not prices:
            if len(codes) == 1:
                last_code = codes[0]
                last_code_idx = cluster_idx
            else:
                last_code = None
                last_code_idx = None
            continue
        if not codes and prices:
            if (
                len(prices) == 1
                and last_code
                and last_code_idx is not None
                and (cluster_idx - last_code_idx) <= 2
                and last_code["token"] not in used_codes
            ):
                code_pick = last_code
                price_pick = prices[0]
                used_codes.add(code_pick["token"])
                currency = price_pick.get("currency") or (target_currency or "").upper()
                line_parts = [code_pick["token"]]
                if currency:
                    line_parts.append(currency)
                line_parts.append(price_pick["token"])
                rows.append(
                    {
                        "line_text": " ".join(line_parts),
                        "ambiguous": False,
                        "code_count": len(codes),
                        "price_count": len(prices),
                        "windowed": True,
                    }
                )
                rows_built += 1
            continue
        code_pick, price_pick, ambiguous = _select_pair(codes, prices)
        if not code_pick or not price_pick:
            continue
        name_tokens = _name_tokens(tokens, code_pick["idx"], price_pick["idx"])
        line_parts = [code_pick["token"]]
        if name_tokens:
            line_parts.extend(name_tokens)
        currency = price_pick.get("currency") or (target_currency or "").upper()
        if currency:
            line_parts.append(currency)
        line_parts.append(price_pick["token"])
        rows.append(
            {
                "line_text": " ".join(line_parts),
                "ambiguous": ambiguous,
                "code_count": len(codes),
                "price_count": len(prices),
                "windowed": False,
            }
        )
        rows_built += 1
        if ambiguous:
            rows_ambiguous += 1
        if code_pick.get("token"):
            used_codes.add(code_pick["token"])
        last_code = None
        last_code_idx = None
    return rows, {"rows_built": rows_built, "rows_ambiguous": rows_ambiguous}


def _cluster_by_line(words: List[dict], y_tolerance: float) -> List[List[dict]]:
    def y_value(word: dict) -> float:
        y = word.get("top")
        if y is None:
            y = word.get("doctop")
        return float(y or 0.0)

    words_sorted = sorted(
        words, key=lambda item: (y_value(item), float(item.get("x0") or 0.0))
    )
    clusters: List[List[dict]] = []
    current: List[dict] = []
    current_y = None
    for word in words_sorted:
        y = y_value(word)
        if current_y is None or abs(y - current_y) <= y_tolerance:
            current.append(word)
            current_y = y if current_y is None else (current_y + y) / 2.0
        else:
            clusters.append(current)
            current = [word]
            current_y = y
    if current:
        clusters.append(current)
    return clusters


def _tokens_from_cluster(cluster: List[dict]) -> List[dict]:
    tokens = []
    for idx, word in enumerate(
        sorted(cluster, key=lambda item: float(item.get("x0") or 0.0))
    ):
        text = (word.get("text") or "").strip()
        if not text:
            continue
        tokens.append(
            {
                "idx": idx,
                "text": text,
                "x0": float(word.get("x0") or 0.0),
            }
        )
    return tokens


def _code_candidates(tokens: List[dict], code_label_present: bool, spec_context: bool) -> List[dict]:
    candidates = []
    for token in tokens:
        value = text_utils.canonicalize_art_no(token["text"])
        if not value:
            continue
        if text_utils.is_short_grade_token(value) and not code_label_present:
            continue
        if spec_context and not code_label_present and len(value) <= 5:
            continue
        if text_utils.is_art_no_candidate_token(value) or (
            code_label_present and text_utils.is_short_grade_token(value)
        ):
            has_alpha = bool(re.search(r"[A-Za-z]", value))
            has_digit = bool(re.search(r"\d", value))
            kind = "alnum" if has_alpha and has_digit else "numeric"
            candidates.append(
                {
                    "token": value,
                    "x0": token["x0"],
                    "idx": token["idx"],
                    "kind": kind,
                }
            )
    if any(candidate["kind"] == "alnum" for candidate in candidates):
        return [candidate for candidate in candidates if candidate["kind"] == "alnum"]
    return candidates


def _price_candidates(tokens: List[dict]) -> List[dict]:
    candidates = []
    currency_indices = {
        token["idx"]
        for token in tokens
        if text_utils.is_currency_token(token["text"])
    }
    unit_indices = {
        token["idx"]
        for token in tokens
        if _is_unit_token(token["text"])
    }
    for token in tokens:
        idx = token["idx"]
        if idx in currency_indices or idx in unit_indices:
            continue
        raw_text = token["text"]
        if any(char.isalpha() for char in raw_text) and not (
            text_utils.is_currency_token(raw_text) or text_utils.token_has_currency(raw_text)
        ):
            continue
        cleaned = text_utils.strip_currency_token(raw_text)
        if not text_utils._PRICE_TOKEN_RE.match(cleaned):
            continue
        if not cleaned or not any(char.isdigit() for char in cleaned):
            continue
        if text_utils.is_year_price_candidate(cleaned, token["text"]):
            continue
        if any(adj in unit_indices for adj in (idx - 1, idx + 1)):
            continue
        value = text_utils.parse_price(cleaned)
        if value is None:
            continue
        currency = ""
        for adj in (idx - 1, idx + 1, idx):
            if adj in currency_indices:
                currency = tokens[adj]["text"].upper()
        if text_utils.token_has_currency(token["text"]):
            upper = token["text"].upper()
            if "EUR" in upper or "\u20ac" in token["text"]:
                currency = "EUR"
            elif "DKK" in upper:
                currency = "DKK"
            elif "SEK" in upper:
                currency = "SEK"
            elif "NOK" in upper:
                currency = "NOK"
        candidates.append(
            {
                "token": cleaned,
                "value": value,
                "x0": token["x0"],
                "idx": idx,
                "currency": currency,
            }
        )
    return candidates


def _select_pair(
    codes: List[dict],
    prices: List[dict],
) -> Tuple[dict, dict, bool]:
    ambiguous = len(codes) != 1 or len(prices) != 1
    best_pair = None
    best_distance = None
    for code in codes:
        for price in prices:
            distance = price["x0"] - code["x0"]
            if distance < 0:
                distance = abs(distance) + 10000.0
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_pair = (code, price)
    if not best_pair:
        return {}, {}, True
    return best_pair[0], best_pair[1], ambiguous


def _name_tokens(tokens: List[dict], code_idx: int, price_idx: int) -> List[str]:
    if code_idx is None or price_idx is None:
        return []
    start = min(code_idx, price_idx) + 1
    end = max(code_idx, price_idx)
    name_tokens = []
    for token in tokens[start:end]:
        text = token["text"]
        if text_utils.is_currency_token(text):
            continue
        if text_utils.is_number_token(text):
            continue
        if not any(char.isalpha() for char in text):
            continue
        name_tokens.append(text)
    return name_tokens


def _is_unit_token(token: str) -> bool:
    if not token:
        return False
    lower = token.lower()
    if lower in _UNIT_TOKENS:
        return True
    if '"' in token:
        return True
    return False
