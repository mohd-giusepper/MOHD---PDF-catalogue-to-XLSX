import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import pdfplumber

from pdf2xlsx import config
from pdf2xlsx.core import extract, normalize
from pdf2xlsx.utils import text as text_utils


_NUMBER_RE = re.compile(r"\d")
_MULTI_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")
_CURRENCY_TOKEN_RE = re.compile(r"\b[A-Z]{3}\b")
_KNOWN_CURRENCIES = {"EUR", "DKK", "SEK", "NOK", "USD", "GBP", "CHF", "JPY", "CAD", "AUD"}
_CODE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-./]*")


@dataclass
class CachedPage:
    page_number: int
    text: str
    normalized_text: str
    lines: List[str]
    words: List[dict]
    text_len: int
    images_count: int
    needs_ocr: bool
    ocr_used: bool
    table_hint: bool
    numeric_density: float = 0.0
    currency_tokens: Dict[str, int] = field(default_factory=dict)
    table_likelihood: float = 0.0
    signal_score: float = 0.0
    mixed_code_count: int = 0
    price_like_count: int = 0
    cooccurrence_count: int = 0


def build_sample_cache(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool = False,
) -> Tuple[List[CachedPage], List[str]]:
    stopwords_lower = [word.lower() for word in stopwords]
    selected: List[CachedPage] = []
    candidates: List[Tuple[CachedPage, bool]] = []
    notes: List[str] = []
    stopword_hits = 0
    ocr_candidates = 0

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for idx in range(total_pages):
            page_number = idx + 1
            page = pdf.pages[idx]
            text = page.extract_text() or ""
            text_len = len(text.strip())
            lower_text = text.lower()
            has_stopword = any(word in lower_text for word in stopwords_lower)
            images_count = len(getattr(page, "images", []))
            needs_ocr = extract.page_needs_ocr(text_len, images_count)
            ocr_used = False

            if ocr and needs_ocr:
                ocr_text = extract.ocr_page(pdf_path, page_number)
                if ocr_text:
                    text = ocr_text
                    text_len = len(text.strip())
                    ocr_used = True

            normalized = normalize.normalize_text(text)
            lines = normalize.split_lines(normalized)
            table_hint = _table_hint(lines)
            words = []
            if not ocr_used and table_hint:
                words = page.extract_words() or []

            cached_page = CachedPage(
                page_number=page_number,
                text=text,
                normalized_text=normalized,
                lines=lines,
                words=words,
                text_len=text_len,
                images_count=images_count,
                needs_ocr=needs_ocr,
                ocr_used=ocr_used,
                table_hint=table_hint,
            )
            candidates.append((cached_page, has_stopword))

            if has_stopword:
                stopword_hits += 1

            if text_len >= min_text_len and not has_stopword:
                selected.append(cached_page)
            elif ocr and needs_ocr and not has_stopword:
                selected.append(cached_page)
                ocr_candidates += 1

            if len(selected) >= max_pages:
                break

    if not selected and candidates:
        candidates_sorted = sorted(candidates, key=lambda item: item[0].text_len, reverse=True)
        selected = [item[0] for item in candidates_sorted[:max_pages]]
        notes.append("fallback_pages_used")

    if stopword_hits:
        notes.append("stopword_pages_skipped")
    if ocr_candidates:
        notes.append("ocr_candidate_pages")

    return selected, notes


def build_signal_cache(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool = False,
) -> Tuple[List[CachedPage], List[str]]:
    stopwords_lower = [word.lower() for word in stopwords]
    candidates: List[Tuple[CachedPage, bool]] = []
    notes: List[str] = []
    stopword_hits = 0

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for idx in range(total_pages):
            page_number = idx + 1
            page = pdf.pages[idx]
            text = page.extract_text() or ""
            text_len = len(text.strip())
            lower_text = text.lower()
            has_stopword = any(word in lower_text for word in stopwords_lower)
            images_count = len(getattr(page, "images", []))
            needs_ocr = extract.page_needs_ocr(text_len, images_count)
            ocr_used = False

            if ocr and needs_ocr:
                ocr_text = extract.ocr_page(pdf_path, page_number)
                if ocr_text:
                    text = ocr_text
                    text_len = len(text.strip())
                    ocr_used = True

            normalized = normalize.normalize_text(text)
            lines = normalize.split_lines(normalized)
            table_hint = _table_hint(lines)
            numeric_density = _numeric_density(normalized)
            currency_tokens = _collect_currency_tokens(text)
            mixed_code_count = 0
            price_like_count = 0
            cooccurrence_count = 0
            for line in lines:
                if not line:
                    continue
                line_info = text_utils.analyze_line(line)
                price_like = bool(line_info.get("price_like"))
                if price_like:
                    price_like_count += 1
                code_count = 0
                for token in _CODE_TOKEN_RE.findall(line):
                    if text_utils.is_plausible_code(token, min_len=config.CODE_MIN_LEN):
                        code_count += 1
                if code_count:
                    mixed_code_count += code_count
                if price_like and code_count:
                    cooccurrence_count += 1

            signal_score = (
                cooccurrence_count * 4.0
                + price_like_count * 2.0
                + mixed_code_count * 1.0
                + sum(currency_tokens.values()) * 1.5
                + numeric_density * 40.0
            )
            if table_hint:
                signal_score += 5.0
            if has_stopword:
                signal_score *= 0.2

            cached_page = CachedPage(
                page_number=page_number,
                text=text,
                normalized_text=normalized,
                lines=lines,
                words=[],
                text_len=text_len,
                images_count=images_count,
                needs_ocr=needs_ocr,
                ocr_used=ocr_used,
                table_hint=table_hint,
                numeric_density=round(numeric_density, 4),
                currency_tokens=currency_tokens,
                table_likelihood=0.0,
                signal_score=round(signal_score, 4),
                mixed_code_count=mixed_code_count,
                price_like_count=price_like_count,
                cooccurrence_count=cooccurrence_count,
            )
            candidates.append((cached_page, has_stopword))
            if has_stopword:
                stopword_hits += 1

    if not candidates:
        return [], []

    eligible = [item for item in candidates if item[0].text_len >= min_text_len and not item[1]]
    if eligible:
        eligible.sort(key=lambda item: item[0].signal_score, reverse=True)
        selected = [item[0] for item in eligible[:max_pages]]
    else:
        candidates.sort(key=lambda item: item[0].signal_score, reverse=True)
        selected = [item[0] for item in candidates[:max_pages]]
        notes.append("fallback_pages_used")

    if stopword_hits:
        notes.append("stopword_pages_skipped")

    selected_numbers = {page.page_number for page in selected}
    with pdfplumber.open(pdf_path) as pdf:
        for page in selected:
            pdf_page = pdf.pages[page.page_number - 1]
            words = []
            if page.table_hint:
                words = pdf_page.extract_words() or []
            columns, ratio = _table_metrics_from_words(words)
            page.words = words
            page.table_likelihood = round(columns + ratio * 5.0, 3)

    return selected, notes


def _table_hint(lines: List[str]) -> bool:
    if not lines:
        return False

    numeric_lines = 0
    spaced_lines = 0
    for line in lines:
        if not line:
            continue
        if _MULTI_NUMBER_RE.findall(line) and len(_MULTI_NUMBER_RE.findall(line)) >= 2:
            numeric_lines += 1
        if "  " in line:
            spaced_lines += 1

    total = len(lines)
    numeric_ratio = numeric_lines / total if total else 0.0
    spaced_ratio = spaced_lines / total if total else 0.0
    if numeric_ratio >= config.TABLE_WORDS_HINT_MIN_RATIO:
        return True
    if spaced_ratio >= config.TABLE_WORDS_HINT_MIN_SPACE_RATIO:
        return True
    return False


def _table_metrics_from_words(words: List[dict]) -> Tuple[int, float]:
    if not words:
        return 0, 0.0

    bin_size = config.TRIAGE_TABLE_X_BIN_SIZE
    y_bin_size = config.TRIAGE_TABLE_Y_BIN_SIZE
    bin_counts: Counter[int] = Counter()
    for word in words:
        x0 = word.get("x0")
        if x0 is None:
            continue
        bin_id = int(x0 // bin_size)
        bin_counts[bin_id] += 1

    strong_bins = {
        bin_id
        for bin_id, count in bin_counts.items()
        if count >= config.TRIAGE_TABLE_MIN_BIN_HITS
    }
    column_count = len(strong_bins)
    if column_count == 0:
        return 0, 0.0

    line_bins: defaultdict[int, List[dict]] = defaultdict(list)
    for word in words:
        top = word.get("top")
        if top is None:
            continue
        line_id = int(top // y_bin_size)
        line_bins[line_id].append(word)

    numeric_line_count = 0
    for line_words in line_bins.values():
        numeric_bins = set()
        for word in line_words:
            text = (word.get("text") or "").strip()
            if not text or not _NUMBER_RE.search(text):
                continue
            x0 = word.get("x0")
            if x0 is None:
                continue
            bin_id = int(x0 // bin_size)
            if bin_id in strong_bins:
                numeric_bins.add(bin_id)
        if len(numeric_bins) >= 2:
            numeric_line_count += 1

    total_lines = len(line_bins)
    ratio = numeric_line_count / total_lines if total_lines else 0.0
    return column_count, ratio


def _numeric_density(text: str) -> float:
    if not text:
        return 0.0
    digits = sum(1 for char in text if char.isdigit())
    return digits / len(text) if text else 0.0


def _collect_currency_tokens(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    if not text:
        return counts
    euro_symbol = text.count("\u20ac")
    if euro_symbol:
        counts["EUR_symbol"] = euro_symbol
    for token in _CURRENCY_TOKEN_RE.findall(text):
        if token in _KNOWN_CURRENCIES:
            counts[token] = counts.get(token, 0) + 1
    return counts
