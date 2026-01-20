import logging
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from pdf2xlsx import config
from pdf2xlsx.core import page_cache
from pdf2xlsx.models import TriageResult
from pdf2xlsx.utils import labels as label_utils


LOGGER = logging.getLogger(__name__)

CURRENCY_RE = re.compile(r"\b(?:EUR|DKK|SEK|NOK)\b", re.IGNORECASE)
EURO_RE = re.compile(r"\u20ac")
PRICE_RE = re.compile(
    r"\b\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?\b|\b\d{1,7}(?:[.,]\d{1,2})\b"
)

STRATEGY_PARSER_MAP = {
    "stelton_marker": "stelton_2025",
    "table_based": "table_based",
    "code_price_based": "code_price_based",
}


def scan_folder(
    input_dir: str,
    ocr: bool = False,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> List[TriageResult]:
    folder = Path(input_dir)
    pdfs = sorted(folder.glob("*.pdf"))
    results: List[TriageResult] = []
    total = len(pdfs)

    for idx, pdf_path in enumerate(pdfs, start=1):
        if should_stop and should_stop():
            LOGGER.info("Triage scan stopped by user.")
            break
        if progress_callback:
            progress_callback(idx - 1, total, pdf_path.name)
        try:
            result = scan_pdf(str(pdf_path), ocr=ocr)
        except Exception as exc:
            LOGGER.exception("Triage failed for %s", pdf_path.name)
            result = TriageResult(
                source_file=pdf_path.name,
                source_path=str(pdf_path),
                decision="NO",
                suggested_profile="error",
                reasons=f"triage_error:{exc}",
            )
        results.append(result)
        if progress_callback:
            progress_callback(idx, total, pdf_path.name)

    return results


def scan_folder_recursive(
    input_dir: str,
    ocr: bool = False,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> List[TriageResult]:
    folder = Path(input_dir)
    pdfs = sorted(folder.rglob("*.pdf"))
    results: List[TriageResult] = []
    total = len(pdfs)

    for idx, pdf_path in enumerate(pdfs, start=1):
        if should_stop and should_stop():
            LOGGER.info("Triage scan stopped by user.")
            break
        display_name = str(pdf_path.relative_to(folder))
        if progress_callback:
            progress_callback(idx - 1, total, display_name)
        try:
            result, _ = scan_pdf_cached(str(pdf_path), ocr=ocr)
            result.source_file = display_name
            result.source_path = str(pdf_path)
        except Exception as exc:
            LOGGER.exception("Triage failed for %s", display_name)
            result = TriageResult(
                source_file=display_name,
                source_path=str(pdf_path),
                decision="NO",
                suggested_profile="error",
                reasons=f"triage_error:{exc}",
            )
        results.append(result)
        if progress_callback:
            progress_callback(idx, total, display_name)

    return results


def scan_pdf(pdf_path: str, ocr: bool = False) -> TriageResult:
    result, _ = scan_pdf_cached(pdf_path, ocr=ocr)
    return result


def scan_pdf_cached(
    pdf_path: str,
    ocr: bool = False,
) -> Tuple[TriageResult, List[page_cache.CachedPage]]:
    label_dict = label_utils.load_label_dictionary()
    marker_dict = label_utils.load_profile_dictionary("stelton_marker")
    code_dict = label_utils.load_profile_dictionary("code_price_based")
    marker_patterns = label_utils.build_label_patterns(marker_dict.get("fields", {}))
    code_patterns = label_utils.build_label_patterns(code_dict.get("fields", {}))
    stopwords = label_dict.get("stopwords") or config.TRIAGE_STOPWORDS

    cached_pages, page_notes, _ = page_cache.build_signal_cache(
        pdf_path,
        max_pages=config.TRIAGE_TOP_K_MAX,
        min_text_len=config.TRIAGE_TEXT_LEN_MIN,
        stopwords=stopwords,
        ocr=ocr,
    )
    result = scan_cached_pages(
        pdf_path=pdf_path,
        cached_pages=cached_pages,
        page_notes=page_notes,
        marker_patterns=marker_patterns,
        code_patterns=code_patterns,
    )
    return result, cached_pages


def scan_cached_pages(
    pdf_path: str,
    cached_pages: List[page_cache.CachedPage],
    page_notes: List[str],
    marker_patterns: dict,
    code_patterns: dict,
) -> TriageResult:
    if not cached_pages:
        return TriageResult(
            source_file=Path(pdf_path).name,
            source_path=pdf_path,
            decision="NO",
            suggested_profile="unknown",
            reasons="no_content_pages",
        )

    pages = [page.page_number for page in cached_pages]
    signals = {
        "art_no": 0,
        "rrp": 0,
        "colli": 0,
        "designer": 0,
        "code": 0,
        "description": 0,
        "price_label": 0,
        "currency": 0,
        "euro": 0,
        "price": 0,
    }
    table_columns = 0
    numeric_line_ratios: List[float] = []
    text_len_total = 0
    ocr_needed_pages = 0
    ocr_used_pages = 0

    for page in cached_pages:
        if page.needs_ocr:
            ocr_needed_pages += 1
        if page.ocr_used:
            ocr_used_pages += 1
        text = page.normalized_text or ""
        text_len_total += page.text_len
        signals["art_no"] += label_utils.count_label_hits(marker_patterns, "art_no", text)
        signals["rrp"] += label_utils.count_label_hits(marker_patterns, "rrp", text)
        signals["colli"] += label_utils.count_label_hits(marker_patterns, "colli", text)
        signals["designer"] += label_utils.count_label_hits(marker_patterns, "designer", text)
        signals["code"] += label_utils.count_label_hits(code_patterns, "code", text)
        signals["description"] += label_utils.count_label_hits(code_patterns, "description", text)
        signals["price_label"] += label_utils.count_label_hits(code_patterns, "price", text)
        signals["currency"] += len(CURRENCY_RE.findall(text))
        signals["euro"] += len(EURO_RE.findall(text))
        signals["price"] += len(PRICE_RE.findall(text))

        columns, ratio = table_metrics_from_words(page.words)
        table_columns = max(table_columns, columns)
        numeric_line_ratios.append(ratio)

    numeric_line_ratio = (
        sum(numeric_line_ratios) / len(numeric_line_ratios)
        if numeric_line_ratios
        else 0.0
    )

    marker_score = (
        signals["rrp"] * 2
        + signals["art_no"] * 2
        + signals["colli"]
        + signals["designer"]
    )
    table_score = 0.0
    if table_columns >= config.TRIAGE_TABLE_MIN_COLUMNS:
        table_score += float(table_columns)
    table_score += numeric_line_ratio * 5.0
    code_price_score = (
        (signals["code"] + signals["description"]) * 2
        + signals["price_label"]
        + signals["price"]
        + signals["currency"]
        + signals["euro"]
    )

    suggested_profile, support_score = choose_profile(
        marker_score=marker_score,
        table_score=table_score,
        code_price_score=code_price_score,
        art_no_count=signals["art_no"],
        rrp_count=signals["rrp"],
        price_count=signals["price"],
        table_columns=table_columns,
    )
    if signals["art_no"] >= 20 and signals["rrp"] >= 20:
        suggested_profile = "stelton_marker"
        support_score = max(support_score, marker_score)
        page_notes.append("forced_marker_profile")
    parser_name = STRATEGY_PARSER_MAP.get(suggested_profile, "")
    decision, decision_notes = choose_decision(
        suggested_profile=suggested_profile,
        parser_name=parser_name,
        pages_sampled=pages,
        text_len_total=text_len_total,
        support_score=support_score,
        ocr_used_pages=ocr_used_pages,
        signals=signals,
        numeric_line_ratio=numeric_line_ratio,
    )

    notes = page_notes + decision_notes
    return TriageResult(
        source_file=Path(pdf_path).name,
        source_path=pdf_path,
        pages_sampled=pages,
        suggested_profile=suggested_profile,
        support_score=support_score,
        decision=decision,
        parser=parser_name,
        marker_score=marker_score,
        table_score=table_score,
        code_price_score=code_price_score,
        art_no_count=signals["art_no"],
        rrp_count=signals["rrp"],
        colli_count=signals["colli"],
        designer_count=signals["designer"],
        code_label_count=signals["code"],
        description_label_count=signals["description"],
        price_label_count=signals["price_label"],
        price_count=signals["price"],
        euro_count=signals["euro"],
        currency_code_count=signals["currency"],
        table_columns=table_columns,
        numeric_line_ratio=numeric_line_ratio,
        text_len_total=text_len_total,
        ocr_needed_pages=ocr_needed_pages,
        ocr_used_pages=ocr_used_pages,
        reasons="; ".join(note for note in notes if note),
    )


def table_metrics_from_words(words: List[dict]) -> Tuple[int, float]:
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
            if not text or not re.search(r"\d", text):
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


def choose_profile(
    marker_score: float,
    table_score: float,
    code_price_score: float,
    art_no_count: int,
    rrp_count: int,
    price_count: int,
    table_columns: int,
) -> Tuple[str, float]:
    support_score = max(marker_score, table_score, code_price_score)
    candidates: List[Tuple[str, float]] = []
    if art_no_count >= 20 and rrp_count >= 20:
        return "stelton_marker", max(support_score, marker_score)
    if (
        marker_score >= config.TRIAGE_MARKER_SCORE_MIN
        and art_no_count >= config.TRIAGE_MARKER_MIN_COUNT
        and rrp_count >= config.TRIAGE_MARKER_MIN_COUNT
    ):
        candidates.append(("stelton_marker", marker_score))
    if (
        table_score >= config.TRIAGE_TABLE_SCORE_MIN
        and table_columns >= config.TRIAGE_TABLE_MIN_COLUMNS
    ):
        candidates.append(("table_based", table_score))
    if (
        code_price_score >= config.TRIAGE_CODE_PRICE_SCORE_MIN
        and price_count >= config.TRIAGE_PRICE_MIN_COUNT
    ):
        candidates.append(("code_price_based", code_price_score))
    if not candidates:
        return "unknown", support_score
    candidates.sort(key=lambda item: item[1], reverse=True)
    return candidates[0][0], support_score


def choose_decision(
    suggested_profile: str,
    parser_name: str,
    pages_sampled: List[int],
    text_len_total: int,
    support_score: float,
    ocr_used_pages: int,
    signals: dict,
    numeric_line_ratio: float,
) -> Tuple[str, List[str]]:
    notes: List[str] = []
    if not pages_sampled:
        return "NO", ["no_pages_sampled"]

    has_any_signal = any(
        value > 0
        for value in (
            signals.get("art_no", 0),
            signals.get("rrp", 0),
            signals.get("price", 0),
            signals.get("code", 0),
            signals.get("description", 0),
            support_score,
        )
    )
    if text_len_total < config.TRIAGE_TEXT_LEN_MIN and not has_any_signal:
        return "NO", ["low_text"]

    if ocr_used_pages > 0:
        notes.append("ocr_used")

    if suggested_profile != "unknown" and parser_name:
        if suggested_profile == "table_based":
            if (
                signals.get("price", 0) < config.TRIAGE_PRICE_SIGNAL_MIN_FOR_OK
                and signals.get("euro", 0) < config.TRIAGE_PRICE_SIGNAL_MIN_FOR_OK
                and signals.get("currency", 0) < config.TRIAGE_PRICE_SIGNAL_MIN_FOR_OK
                and numeric_line_ratio < config.TRIAGE_TABLE_NUMERIC_RATIO_MIN_FOR_OK
            ):
                return "FORSE", notes + ["weak_table_signals"]
        if ocr_used_pages == 0:
            return "OK", notes
        return "FORSE", notes

    if suggested_profile != "unknown":
        return "FORSE", notes + ["unsupported_profile"]

    if has_any_signal:
        return "FORSE", notes + ["unknown_profile"]

    return "NO", notes
