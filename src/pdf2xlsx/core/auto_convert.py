import logging
import re
import time
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import pdfplumber

from pdf2xlsx import config
from pdf2xlsx.core import normalize, page_cache, pipeline, scoring, triage
from pdf2xlsx.io import xlsx_writer
from pdf2xlsx.io.run_debug import RunDebugCollector
from pdf2xlsx.models import TriageResult
from pdf2xlsx.parsers import get_parser
from pdf2xlsx.utils import labels as label_utils
from pdf2xlsx.utils import text as text_utils


LOGGER = logging.getLogger(__name__)

PROFILE_ORDER = ["stelton_marker", "table_based", "code_price_based"]
PROFILE_PARSER_MAP = {
    "stelton_marker": "stelton_2025",
    "table_based": "table_based",
    "code_price_based": "code_price_based",
}
CURRENCY_CODES = ("EUR", "DKK", "SEK", "NOK")
CURRENCY_PRICE_RE = re.compile(
    r"\b(EUR|DKK|SEK|NOK)\s+([0-9]+(?:[.,][0-9]{1,2})?)",
    re.IGNORECASE,
)
RETRY_REASONS = {
    "too_few_rows",
    "no_valid_rows_found",
    "low_unique_code_ratio",
    "low_key_fields_rate",
    "table_precheck_failed",
    "early_discard_low_unique",
    "early_discard_low_key_fields",
}


def detect_target_currency(cached_pages, fallback: str) -> Tuple[str, float, dict, bool]:
    counts = {code: 0 for code in CURRENCY_CODES}
    for page in cached_pages:
        text = page.normalized_text or ""
        for match in CURRENCY_PRICE_RE.finditer(text):
            code = match.group(1).upper()
            counts[code] += 1
    total = sum(counts.values())
    multi_currency = sum(1 for value in counts.values() if value > 0) >= 2
    if total == 0:
        return fallback, 0.0, counts, False
    best = max(counts, key=counts.get)
    ratio = counts[best] / total if total else 0.0
    if total < config.CURRENCY_AUTO_MIN_COUNT or ratio < config.CURRENCY_AUTO_MIN_RATIO:
        return fallback, ratio, counts, multi_currency
    return best, ratio, counts, multi_currency


def resolve_target_currency(
    cached_pages,
    fallback_currency: str,
    explicit_currency: str,
) -> Tuple[str, float, dict, bool]:
    explicit = (explicit_currency or "").upper()
    fallback = (fallback_currency or config.TARGET_CURRENCY).upper()
    if explicit in CURRENCY_CODES:
        return explicit, 1.0, {}, True
    detected_currency, currency_confidence, currency_counts, multi_currency = (
        detect_target_currency(cached_pages, fallback)
    )
    if multi_currency and "EUR" in currency_counts and currency_counts["EUR"] > 0:
        target_currency = "EUR"
    else:
        target_currency = detected_currency
    filter_currency = multi_currency
    return target_currency, currency_confidence, currency_counts, filter_currency


def should_retry_fast_eval(attempt_results: List[dict]) -> str:
    for result in attempt_results:
        reason = result.get("reason")
        if reason in RETRY_REASONS:
            return reason
    return ""


def _should_retry_cooccurrence(cached_pages, cache_meta: dict) -> bool:
    if not isinstance(cache_meta, dict):
        return False
    if (
        "cooccurrence_min_required" not in cache_meta
        and "cooccurrence_pages_selected" not in cache_meta
    ):
        return False
    min_required = int(
        cache_meta.get("cooccurrence_min_required", config.CACHE_MIN_COOCCURRENCE_PAGES)
    )
    if min_required <= 0:
        return False
    if cache_meta.get("exclude_zero_cooccurrence_pages"):
        return False
    selected = cache_meta.get("cooccurrence_pages_selected")
    if selected is None:
        selected = sum(
            1
            for page in cached_pages
            if getattr(page, "cooccurrence_count", 0) > 0
            or getattr(page, "cooccurrence_near_count", 0) > 0
        )
    return selected < min_required


def _all_attempts_reason(attempt_results: List[dict], reason: str) -> bool:
    if not attempt_results:
        return False
    return all(item.get("reason") == reason for item in attempt_results)


def _all_attempts_zero_rows(attempt_results: List[dict]) -> bool:
    if not attempt_results:
        return False
    for attempt in attempt_results:
        metrics = attempt.get("metrics", {}) or {}
        eval_rows = metrics.get("eval_rows")
        if eval_rows is None:
            eval_rows = metrics.get("rows_exported")
        if eval_rows is None:
            return False
        if int(eval_rows or 0) > 0:
            return False
    return True


def _eval_rows_from_attempt(attempt: dict) -> int:
    metrics = attempt.get("metrics", {}) or {}
    eval_rows = metrics.get("eval_rows")
    if eval_rows is None:
        eval_rows = metrics.get("rows_exported")
    return int(eval_rows or 0)


def select_review_only_attempt(
    attempt_results: List[dict],
    triage_result: Optional[TriageResult],
) -> Optional[dict]:
    candidates = [item for item in attempt_results if _eval_rows_from_attempt(item) > 0]
    if not candidates:
        return None
    preferred_parser = ""
    if triage_result and triage_result.suggested_profile:
        preferred_parser = PROFILE_PARSER_MAP.get(triage_result.suggested_profile, "")
    if preferred_parser:
        for item in candidates:
            if item.get("parser") == preferred_parser:
                return item
    candidates.sort(
        key=lambda item: (
            _eval_rows_from_attempt(item),
            item.get("metrics", {}).get("eval_score", 0.0),
            item.get("score", 0.0),
        ),
        reverse=True,
    )
    return candidates[0]


def _sort_eval_pages(
    pages: List[page_cache.CachedPage],
) -> List[page_cache.CachedPage]:
    def sort_key(page: page_cache.CachedPage) -> Tuple[int, int, int, int, float]:
        return (
            int(getattr(page, "row_candidate_count", 0) or 0),
            int(getattr(page, "cooccurrence_near_count", 0) or 0),
            int(getattr(page, "cooccurrence_count", 0) or 0),
            int(getattr(page, "mixed_code_count", 0) or 0),
            float(getattr(page, "signal_score", 0.0) or 0.0),
        )

    return sorted(pages, key=sort_key, reverse=True)


def _ensure_cache_meta(
    pdf_path: str,
    cached_pages: List[page_cache.CachedPage],
    cache_meta: Optional[dict],
) -> dict:
    meta = dict(cache_meta or {})
    pages_sampled = meta.get("pages_sampled")
    if not pages_sampled:
        pages_sampled = [page.page_number for page in cached_pages if page.page_number]
        meta["pages_sampled"] = pages_sampled
    if not meta.get("sample_count"):
        meta["sample_count"] = len(pages_sampled or [])
    if not meta.get("top_k_pages"):
        meta["top_k_pages"] = list(pages_sampled or [])
    if not meta.get("top_k_scores"):
        meta["top_k_scores"] = [
            float(getattr(page, "signal_score", 0.0) or 0.0) for page in cached_pages
        ]
    if not meta.get("num_pages"):
        total_pages = 0
        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
        except Exception:
            total_pages = max(pages_sampled or [0])
            if not total_pages and cached_pages:
                total_pages = len(cached_pages)
        meta["num_pages"] = total_pages
    if not meta.get("scan_mode"):
        meta["scan_mode"] = "provided_cache" if cached_pages else ""
    if "toc_like_pages_candidate" not in meta:
        meta["toc_like_pages_candidate"] = sum(
            1 for page in cached_pages if getattr(page, "toc_like", False)
        )
    if "toc_hard_pages_candidate" not in meta:
        meta["toc_hard_pages_candidate"] = sum(
            1 for page in cached_pages if getattr(page, "toc_hard", False)
        )
    if "toc_hard_pages_excluded" not in meta:
        meta["toc_hard_pages_excluded"] = 0
    if "top_k_min_target" not in meta:
        meta["top_k_min_target"] = 0
    if "top_k_reintegrated" not in meta:
        meta["top_k_reintegrated"] = False
    if "top_k_reintegrated_count" not in meta:
        meta["top_k_reintegrated_count"] = 0
    if "top_k_collapse_reason" not in meta:
        meta["top_k_collapse_reason"] = ""
    if "export_policy_mode" not in meta:
        meta["export_policy_mode"] = config.EXPORT_POLICY_MODE
    if "rows_total" not in meta:
        meta["rows_total"] = 0
    if "rows_exported" not in meta:
        meta["rows_exported"] = 0
    if "rows_review" not in meta:
        meta["rows_review"] = 0
    if "rows_noise" not in meta:
        meta["rows_noise"] = 0
    if "guardrail_counts" not in meta:
        meta["guardrail_counts"] = {}
    return meta


def _inject_report_summary(cache_meta: dict, report) -> dict:
    if not report:
        return cache_meta
    meta = dict(cache_meta or {})
    meta["rows_total"] = len(getattr(report, "rows", []) or [])
    meta["rows_exported"] = int(getattr(report, "rows_exported", 0) or 0)
    meta["rows_review"] = int(getattr(report, "rows_needs_review", 0) or 0)
    guardrail_counts = getattr(report, "guardrail_counts", {}) or {}
    meta["guardrail_counts"] = guardrail_counts
    meta["rows_noise"] = int(guardrail_counts.get("noise_rows", 0) or 0)
    config_info = getattr(report, "config_info", {}) or {}
    if config_info.get("export_policy_mode"):
        meta["export_policy_mode"] = config_info.get("export_policy_mode")
    return meta


def _apply_sampling_debug(
    triage_result: TriageResult,
    cached_pages_source: str,
    retry_count: int,
    retry_reason: str,
    retry_old_meta: Optional[dict] = None,
    retry_new_meta: Optional[dict] = None,
    cache_meta: Optional[dict] = None,
) -> None:
    if not triage_result:
        return
    triage_result.cached_pages_source = cached_pages_source
    triage_result.sampling_retry_triggered = retry_count > 0
    triage_result.sampling_retry_reason = retry_reason or ""
    triage_result.sampling_retry_count = retry_count
    if retry_old_meta:
        triage_result.sampling_retry_old_sample_count = int(
            retry_old_meta.get("sample_count", 0) or 0
        )
        triage_result.sampling_retry_pages_sampled_old = list(
            retry_old_meta.get("pages_sampled", []) or []
        )
    if retry_new_meta:
        triage_result.sampling_retry_new_sample_count = int(
            retry_new_meta.get("sample_count", 0) or 0
        )
        triage_result.sampling_retry_pages_sampled_new = list(
            retry_new_meta.get("pages_sampled", []) or []
        )
    if cache_meta:
        triage_result.toc_like_pages_candidate = int(
            cache_meta.get("toc_like_pages_candidate", 0) or 0
        )
        triage_result.toc_hard_pages_candidate = int(
            cache_meta.get("toc_hard_pages_candidate", 0) or 0
        )
        triage_result.toc_hard_pages_excluded = int(
            cache_meta.get("toc_hard_pages_excluded", 0) or 0
        )
        triage_result.top_k_min_target = int(
            cache_meta.get("top_k_min_target", 0) or 0
        )
        triage_result.top_k_reintegrated = bool(
            cache_meta.get("top_k_reintegrated", False)
        )
        triage_result.top_k_reintegrated_count = int(
            cache_meta.get("top_k_reintegrated_count", 0) or 0
        )
        triage_result.top_k_collapse_reason = str(
            cache_meta.get("top_k_collapse_reason", "") or ""
        )


def write_diagnostic_output(
    output_dir: Path,
    stem: str,
    source_file: str,
    cached_pages: List[page_cache.CachedPage],
    cache_meta: dict,
    attempt_results: List[dict],
) -> str:
    diagnostic_path = output_dir / f"{stem}.diagnostic.xlsx"
    xlsx_writer.write_diagnostic_summary(
        output_path=str(diagnostic_path),
        source_file=source_file,
        cached_pages=cached_pages,
        cache_meta=cache_meta or {},
        attempt_results=attempt_results or [],
    )
    LOGGER.info("DIAGNOSTIC_OUTPUT written=%s", diagnostic_path)
    return str(diagnostic_path)


def run_fast_eval_profiles(
    cached_pages: List[page_cache.CachedPage],
    triage_result: TriageResult,
    target_currency: str,
    filter_currency: bool,
    source_file: str,
) -> Tuple[List[str], List[dict], List[dict]]:
    attempts: List[str] = []
    attempts_detail: List[dict] = []
    attempt_results: List[dict] = []
    ordered_profiles = order_profiles(triage_result)
    for profile_id in ordered_profiles:
        parser_name = PROFILE_PARSER_MAP.get(profile_id, "")
        if not parser_name:
            attempts.append(f"{profile_id}:fail(missing_parser)")
            attempt_results.append(
                {"parser": parser_name, "ok": False, "reason": "missing_parser"}
            )
            continue

        eval_result = evaluate_parser_fast(
            cached_pages=cached_pages,
            parser_name=parser_name,
            source_file=source_file,
            currency=target_currency,
            currency_only=target_currency if filter_currency else "",
            triage_result=triage_result,
        )
        attempt_results.append(eval_result)
        attempts_detail.append(
            {
                "source_file": source_file,
                "parser": parser_name,
                "eval_pages": eval_result.get("metrics", {}).get("eval_pages_used", 0),
                "eval_rows": eval_result.get("metrics", {}).get("eval_rows", 0),
                "eval_time_ms": eval_result.get("metrics", {}).get("eval_time_ms", 0),
                "eval_score": eval_result.get("metrics", {}).get("eval_score", 0.0),
                "status": "ok" if eval_result.get("ok") else "fail",
                "fail_reason": "" if eval_result.get("ok") else eval_result.get("reason", ""),
            }
        )
        attempts.append(
            format_attempt(
                parser_name,
                "ok" if eval_result.get("ok") else "fail",
                eval_result.get("reason", ""),
                eval_result.get("metrics", {}),
                eval_result.get("score"),
            )
        )

        if eval_result.get("ok") and is_excellent(eval_result.get("metrics", {})):
            break

    return attempts, attempts_detail, attempt_results


def run_auto_folder(
    input_dir: str,
    output_dir: str,
    ocr: bool = False,
    currency: str = config.TARGET_CURRENCY,
    currency_only: Optional[str] = None,
    fast_eval_only: bool = False,
    full_pages: Optional[List[int]] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    run_debug: Optional[RunDebugCollector] = None,
) -> List[TriageResult]:
    folder = Path(input_dir)
    pdfs = sorted(folder.glob("*.pdf"))
    results: List[TriageResult] = []
    total = len(pdfs)

    for idx, pdf_path in enumerate(pdfs, start=1):
        if should_stop and should_stop():
            LOGGER.info("Auto-convert stopped by user.")
            break
        if progress_callback:
            progress_callback(idx - 1, total, pdf_path.name)
        result = run_auto_for_pdf(
            pdf_path=str(pdf_path),
            output_dir=output_dir,
            ocr=ocr,
            currency=currency,
            currency_only=currency_only,
            fast_eval_only=fast_eval_only,
            full_pages=full_pages,
            run_debug=run_debug,
        )
        results.append(result)
        if progress_callback:
            progress_callback(idx, total, pdf_path.name)

    if should_stop and should_stop() and len(results) < total:
        for pdf_path in pdfs[len(results) :]:
            results.append(
                TriageResult(
                    source_file=pdf_path.name,
                    source_path=str(pdf_path),
                    final_status="SKIPPED(stopped)",
                    failure_reason="stopped",
                )
            )

    return results


def run_auto_for_pdf(
    pdf_path: str,
    output_dir: str,
    ocr: bool = False,
    currency: str = config.TARGET_CURRENCY,
    currency_only: Optional[str] = None,
    fast_eval_only: bool = False,
    full_pages: Optional[List[int]] = None,
    cached_pages: Optional[List[page_cache.CachedPage]] = None,
    triage_result: Optional[TriageResult] = None,
    debug_enabled: bool = False,
    debug_output_dir: Optional[str] = None,
    run_debug: Optional[RunDebugCollector] = None,
    progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
) -> TriageResult:
    eval_start = time.monotonic()
    label_dict = label_utils.load_label_dictionary()
    marker_dict = label_utils.load_profile_dictionary("stelton_marker")
    code_dict = label_utils.load_profile_dictionary("code_price_based")
    marker_patterns = label_utils.build_label_patterns(marker_dict.get("fields", {}))
    code_patterns = label_utils.build_label_patterns(code_dict.get("fields", {}))
    stopwords = label_dict.get("stopwords") or config.TRIAGE_STOPWORDS

    provided_cache = cached_pages is not None
    sampling_retry_count = 0
    sampling_retry_reason = ""
    sampling_retry_old_meta: Optional[dict] = None
    sampling_retry_new_meta: Optional[dict] = None
    cached_pages_source = "gui_reuse" if provided_cache else "resampled"
    if not provided_cache:
        cached_pages, page_notes, cache_meta = page_cache.build_signal_cache(
            pdf_path,
            max_pages=config.TRIAGE_TOP_K_MAX,
            min_text_len=config.TRIAGE_TEXT_LEN_MIN,
            stopwords=stopwords,
            ocr=ocr,
        )
        if _should_retry_cooccurrence(cached_pages, cache_meta):
            sampling_retry_old_meta = _ensure_cache_meta(
                pdf_path, cached_pages, cache_meta
            )
            old_sample = cache_meta.get("sample_count", len(cached_pages))
            cached_pages, page_notes, cache_meta = page_cache.build_signal_cache(
                pdf_path,
                max_pages=config.TRIAGE_TOP_K_MAX,
                min_text_len=config.TRIAGE_TEXT_LEN_MIN,
                stopwords=stopwords,
                ocr=ocr,
                sample_multiplier=config.CACHE_SAMPLE_RETRY_MULTIPLIER,
                scan_mode="retry_cooc",
                force_rescan=True,
                exclude_zero_cooccurrence_pages=True,
                exclude_toc_pages=True,
            )
            sampling_retry_count += 1
            sampling_retry_reason = "cooccurrence_shortfall"
            cached_pages_source = "resampled"
            sampling_retry_new_meta = _ensure_cache_meta(
                pdf_path, cached_pages, cache_meta
            )
            LOGGER.info(
                "RETRY_SCAN reason=cooccurrence_shortfall old=%s new=%s",
                old_sample,
                cache_meta.get("sample_count", len(cached_pages)),
            )
        triage_result = triage.scan_cached_pages(
            pdf_path=pdf_path,
            cached_pages=cached_pages,
            page_notes=page_notes,
            marker_patterns=marker_patterns,
            code_patterns=code_patterns,
        )
    else:
        page_notes = []
        cache_meta = {}
        if triage_result is None:
            triage_result = triage.scan_cached_pages(
                pdf_path=pdf_path,
                cached_pages=cached_pages,
                page_notes=page_notes,
                marker_patterns=marker_patterns,
                code_patterns=code_patterns,
            )
    cache_meta = _ensure_cache_meta(pdf_path, cached_pages, cache_meta)
    _apply_sampling_debug(
        triage_result,
        cached_pages_source=cached_pages_source,
        retry_count=sampling_retry_count,
        retry_reason=sampling_retry_reason,
        retry_old_meta=sampling_retry_old_meta,
        retry_new_meta=sampling_retry_new_meta,
        cache_meta=cache_meta,
    )

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)
    stem = Path(pdf_path).stem

    fallback_currency = (currency or config.TARGET_CURRENCY).upper()
    explicit_currency = (currency_only or "").upper()
    target_currency, currency_confidence, currency_counts, filter_currency = (
        resolve_target_currency(
            cached_pages=cached_pages,
            fallback_currency=fallback_currency,
            explicit_currency=explicit_currency,
        )
    )
    if explicit_currency not in CURRENCY_CODES:
        LOGGER.info(
            "Auto target currency: %s (confidence=%.2f counts=%s)",
            target_currency,
            currency_confidence,
            currency_counts,
        )
    triage_result.target_currency = target_currency
    triage_result.currency_confidence = currency_confidence
    triage_result.currency_counts = currency_counts

    attempts, attempts_detail, attempt_results = run_fast_eval_profiles(
        cached_pages=cached_pages,
        triage_result=triage_result,
        target_currency=target_currency,
        filter_currency=filter_currency,
        source_file=Path(pdf_path).name,
    )
    selected = select_best_run(attempt_results)

    retry_reason = should_retry_fast_eval(attempt_results)
    retry_exclude_cooc = False
    retry_exclude_toc = False
    retry_scan_mode = "retry"
    if _all_attempts_reason(attempt_results, "early_discard_no_rows"):
        retry_reason = "early_discard_no_rows"
        retry_exclude_cooc = True
        retry_exclude_toc = True
        retry_scan_mode = "retry_weak"
    if _all_attempts_reason(attempt_results, "early_discard_no_rows") or _all_attempts_zero_rows(
        attempt_results
    ):
        retry_reason = "all_parsers_early_discard_no_rows"
        retry_exclude_cooc = False
        retry_exclude_toc = True
        retry_scan_mode = "retry_no_rows"
    force_retry = retry_reason == "all_parsers_early_discard_no_rows"
    if retry_reason and (force_retry or not provided_cache):
        sampling_retry_old_meta = _ensure_cache_meta(
            pdf_path, cached_pages, cache_meta
        )
        old_sample = cache_meta.get("sample_count", len(cached_pages))
        cached_pages, page_notes, cache_meta = page_cache.build_signal_cache(
            pdf_path,
            max_pages=config.TRIAGE_TOP_K_MAX,
            min_text_len=config.TRIAGE_TEXT_LEN_MIN,
            stopwords=stopwords,
            ocr=ocr,
            sample_multiplier=config.CACHE_SAMPLE_RETRY_MULTIPLIER,
            scan_mode=retry_scan_mode,
            force_rescan=True,
            exclude_zero_cooccurrence_pages=retry_exclude_cooc,
            exclude_toc_pages=retry_exclude_toc,
        )
        sampling_retry_count += 1
        sampling_retry_reason = retry_reason
        cached_pages_source = "resampled"
        sampling_retry_new_meta = _ensure_cache_meta(
            pdf_path, cached_pages, cache_meta
        )
        LOGGER.info(
            "RETRY_SCAN reason=%s old=%s new=%s",
            retry_reason,
            old_sample,
            cache_meta.get("sample_count", len(cached_pages)),
        )
        triage_result = triage.scan_cached_pages(
            pdf_path=pdf_path,
            cached_pages=cached_pages,
            page_notes=page_notes,
            marker_patterns=marker_patterns,
            code_patterns=code_patterns,
        )
        cache_meta = _ensure_cache_meta(pdf_path, cached_pages, cache_meta)
        _apply_sampling_debug(
            triage_result,
            cached_pages_source=cached_pages_source,
            retry_count=sampling_retry_count,
            retry_reason=sampling_retry_reason,
            retry_old_meta=sampling_retry_old_meta,
            retry_new_meta=sampling_retry_new_meta,
            cache_meta=cache_meta,
        )
        target_currency, currency_confidence, currency_counts, filter_currency = (
            resolve_target_currency(
                cached_pages=cached_pages,
                fallback_currency=fallback_currency,
                explicit_currency=explicit_currency,
            )
        )
        triage_result.target_currency = target_currency
        triage_result.currency_confidence = currency_confidence
        triage_result.currency_counts = currency_counts
        attempts, attempts_detail, attempt_results = run_fast_eval_profiles(
            cached_pages=cached_pages,
            triage_result=triage_result,
            target_currency=target_currency,
            filter_currency=filter_currency,
            source_file=Path(pdf_path).name,
        )
        selected = select_best_run(attempt_results)

    if (
        not provided_cache
        and not selected
        and config.CACHE_ENABLE_SWEEP_AFTER_RETRY
    ):
        old_sample = cache_meta.get("sample_count", len(cached_pages))
        cached_pages, page_notes, cache_meta = page_cache.build_signal_cache(
            pdf_path,
            max_pages=config.TRIAGE_TOP_K_MAX,
            min_text_len=config.TRIAGE_TEXT_LEN_MIN,
            stopwords=stopwords,
            ocr=ocr,
            sample_multiplier=config.CACHE_SAMPLE_SWEEP_MULTIPLIER,
            scan_mode="sweep",
            force_rescan=True,
            enable_ocr=True,
        )
        sampling_retry_old_meta = _ensure_cache_meta(
            pdf_path, cached_pages, cache_meta
        )
        sampling_retry_count += 1
        sampling_retry_reason = "sweep"
        cached_pages_source = "resampled"
        sampling_retry_new_meta = _ensure_cache_meta(
            pdf_path, cached_pages, cache_meta
        )
        LOGGER.info(
            "RETRY_SCAN reason=sweep old=%s new=%s",
            old_sample,
            cache_meta.get("sample_count", len(cached_pages)),
        )
        triage_result = triage.scan_cached_pages(
            pdf_path=pdf_path,
            cached_pages=cached_pages,
            page_notes=page_notes,
            marker_patterns=marker_patterns,
            code_patterns=code_patterns,
        )
        _apply_sampling_debug(
            triage_result,
            cached_pages_source=cached_pages_source,
            retry_count=sampling_retry_count,
            retry_reason=sampling_retry_reason,
            retry_old_meta=sampling_retry_old_meta,
            retry_new_meta=sampling_retry_new_meta,
        )
        target_currency, currency_confidence, currency_counts, filter_currency = (
            resolve_target_currency(
                cached_pages=cached_pages,
                fallback_currency=fallback_currency,
                explicit_currency=explicit_currency,
            )
        )
        triage_result.target_currency = target_currency
        triage_result.currency_confidence = currency_confidence
        triage_result.currency_counts = currency_counts
        attempts, attempts_detail, attempt_results = run_fast_eval_profiles(
            cached_pages=cached_pages,
            triage_result=triage_result,
            target_currency=target_currency,
            filter_currency=filter_currency,
            source_file=Path(pdf_path).name,
        )
        selected = select_best_run(attempt_results)

    triage_result.eval_time_ms_total = int((time.monotonic() - eval_start) * 1000)
    triage_result.attempts_detail = attempts_detail
    selected = select_best_run(attempt_results)
    if not selected:
        review_candidate = None
        if not fast_eval_only:
            review_candidate = select_review_only_attempt(
                attempt_results, triage_result
            )
        if review_candidate:
            parser_name = review_candidate.get("parser", "")
            review_pages = full_pages or [
                page.page_number for page in cached_pages if page.page_number
            ]
            final_xlsx = output_dir_path / f"{stem}.xlsx"
            report = pipeline.run_pipeline(
                input_pdf=pdf_path,
                output_xlsx=str(final_xlsx),
                pages=review_pages,
                debug_json=None,
                parser_name=parser_name,
                ocr=ocr,
                currency_only=target_currency,
                filter_currency=filter_currency,
                allow_empty_output=True,
                progress_callback=progress_callback,
            )
            apply_report_metrics(triage_result, report)
            triage_result.final_status = f"OK(REVIEW_ONLY,parser={parser_name})"
            triage_result.final_parser = parser_name
            triage_result.winner_parser = parser_name
            triage_result.output_path = str(final_xlsx)
            triage_result.attempts_count = len(attempts)
            triage_result.attempts_summary = "; ".join(attempts)
            triage_result.selection_reason = (
                f"review_only_eval_rows={_eval_rows_from_attempt(review_candidate)}"
            )
            if run_debug:
                run_debug.add_pdf(
                    pdf_path,
                    triage_result,
                    cached_pages,
                    report=report,
                    reason=triage_result.selection_reason,
                )
            return triage_result
        if triage_result.decision == "OK":
            triage_result.decision = "FORSE"
        triage_result.final_status = "FAILED(all_parsers)"
        triage_result.attempts_count = len(attempts)
        triage_result.attempts_summary = "; ".join(attempts)
        triage_result.rows_exported = 0
        triage_result.review_rows = 0
        triage_result.review_rate = 0.0
        triage_result.rows_skipped_missing_target_currency = 0
        triage_result.duplicate_art_no_count = 0
        triage_result.duplicate_conflicts_count = 0
        triage_result.bad_art_no_count = 0
        triage_result.corrected_art_no_count = 0
        triage_result.suspicious_numeric_art_no_seen = False
        triage_result.examples_bad_art_no = []
        if attempts:
            triage_result.failure_reason = extract_last_failure(attempts[-1])
        else:
            triage_result.failure_reason = "no_parsers_available"
        cache_meta = _ensure_cache_meta(pdf_path, cached_pages, cache_meta)
        triage_result.output_path = write_diagnostic_output(
            output_dir=output_dir_path,
            stem=stem,
            source_file=Path(pdf_path).name,
            cached_pages=cached_pages,
            cache_meta=cache_meta,
            attempt_results=attempt_results,
        )
        if run_debug:
            run_debug.add_pdf(
                pdf_path,
                triage_result,
                cached_pages,
                reason=triage_result.failure_reason,
            )
        return triage_result

    parser_name = selected.get("parser", "")
    triage_result.winner_parser = parser_name
    if fast_eval_only:
        metrics = selected.get("metrics", {}) or {}
        triage_result.final_status = f"FAST_EVAL(parser={parser_name})"
        triage_result.attempts_count = len(attempts)
        triage_result.attempts_summary = "; ".join(attempts)
        triage_result.rows_exported = int(metrics.get("rows_exported", 0) or 0)
        triage_result.review_rate = float(metrics.get("review_rate", 0.0) or 0.0)
        triage_result.review_rows = int(
            round(triage_result.rows_exported * triage_result.review_rate)
        )
        triage_result.rows_skipped_missing_target_currency = 0
        triage_result.duplicate_art_no_count = 0
        triage_result.duplicate_conflicts_count = int(
            metrics.get("duplicate_conflicts_count", 0) or 0
        )
        triage_result.bad_art_no_count = 0
        triage_result.corrected_art_no_count = 0
        triage_result.suspicious_numeric_art_no_seen = False
        triage_result.examples_bad_art_no = []
        triage_result.selection_reason = (
            f"fast_eval_score={selected.get('score', 0.0):.3f}"
        )
        if run_debug:
            run_debug.add_pdf(
                pdf_path,
                triage_result,
                cached_pages,
                reason=triage_result.selection_reason,
            )
        return triage_result

    attempt_xlsx = output_dir_path / f"{stem}.{parser_name}.xlsx"
    report = pipeline.run_pipeline(
        input_pdf=pdf_path,
        output_xlsx=str(attempt_xlsx),
        pages=full_pages,
        debug_json=None,
        parser_name=parser_name,
        ocr=ocr,
        currency_only=target_currency,
        filter_currency=filter_currency,
        allow_empty_output=False,
        progress_callback=progress_callback,
    )
    ok, reason, metrics = evaluate_report(report, target_currency)
    apply_report_metrics(triage_result, report)
    if not ok:
        attempts.append(
            format_attempt(parser_name, "fail", reason, metrics, score_run(metrics))
        )
        triage_result.attempts_count = len(attempts)
        triage_result.attempts_summary = "; ".join(attempts)
        triage_result.failure_reason = reason
        rows_exported = metrics.get("rows_exported", 0) or 0
        if rows_exported > 0 and attempt_xlsx.exists():
            final_xlsx = output_dir_path / f"{stem}.xlsx"
            if final_xlsx.exists():
                try:
                    final_xlsx.unlink()
                except OSError:
                    LOGGER.warning("Could not remove existing output %s", final_xlsx)
            try:
                attempt_xlsx.rename(final_xlsx)
            except OSError:
                LOGGER.warning("Could not rename %s to %s", attempt_xlsx, final_xlsx)
            triage_result.final_status = f"PARTIAL(parser={parser_name})"
            triage_result.output_path = str(final_xlsx)
            if run_debug:
                run_debug.add_pdf(
                    pdf_path,
                    triage_result,
                    cached_pages,
                    report=report,
                    reason=reason,
                )
            return triage_result
        triage_result.final_status = "FAILED(selected_parser_failed)"
        if attempt_xlsx.exists():
            try:
                attempt_xlsx.unlink()
            except OSError:
                LOGGER.warning("Could not remove failed output %s", attempt_xlsx)
        cache_meta = _ensure_cache_meta(pdf_path, cached_pages, cache_meta)
        cache_meta = _inject_report_summary(cache_meta, report)
        triage_result.output_path = write_diagnostic_output(
            output_dir=output_dir_path,
            stem=stem,
            source_file=Path(pdf_path).name,
            cached_pages=cached_pages,
            cache_meta=cache_meta,
            attempt_results=attempt_results,
        )
        if run_debug:
            run_debug.add_pdf(
                pdf_path,
                triage_result,
                cached_pages,
                report=report,
                reason=reason,
            )
        return triage_result

    selected["metrics"] = metrics
    selected["score"] = score_run(metrics)
    result = finalize_selection(triage_result, selected, attempts, output_dir_path, stem)
    if run_debug:
        run_debug.add_pdf(
            pdf_path,
            triage_result,
            cached_pages,
            report=report,
            reason=triage_result.selection_reason,
        )
    return result


def order_profiles(triage_result: TriageResult) -> List[str]:
    ordered = []
    if triage_result.art_no_count >= 20 and triage_result.rrp_count >= 20:
        ordered.append("stelton_marker")
    if triage_result.suggested_profile and triage_result.suggested_profile in PROFILE_ORDER:
        if triage_result.suggested_profile not in ordered:
            ordered.append(triage_result.suggested_profile)
    for profile in PROFILE_ORDER:
        if profile not in ordered:
            ordered.append(profile)
    return ordered


def evaluate_report(report, currency: str) -> Tuple[bool, str, dict]:
    metrics = build_metrics_from_report(report, currency)
    return evaluate_metrics(metrics)


def build_metrics_from_report(report, currency: str) -> dict:
    exported = [row for row in report.rows if row.exported]
    rows_exported = len(exported)
    return {
        "rows_exported": rows_exported,
        "unique_code_ratio": compute_unique_code_ratio(exported),
        "key_fields_rate": compute_key_fields_rate(exported, currency),
        "review_rate": compute_review_rate(report),
        "duplicate_conflicts_rate": compute_duplicate_conflicts_rate(report, rows_exported),
        "duplicate_conflicts_count": report.duplicate_conflicts_count,
    }


def evaluate_metrics(metrics: dict) -> Tuple[bool, str, dict]:
    rows_exported = metrics.get("rows_exported", 0) or 0
    eval_pages = metrics.get("eval_pages_used", 0) or 0
    if rows_exported == 0:
        return False, "no_valid_rows_found", metrics
    min_rows = config.AUTO_ROWS_MIN
    if eval_pages and eval_pages <= config.AUTO_EVAL_MIN_PAGES_WEAK:
        min_rows = 1
    if rows_exported < min_rows:
        return False, "too_few_rows", metrics

    if metrics.get("unique_code_ratio", 0.0) < config.AUTO_UNIQUE_CODE_RATIO_MIN:
        return False, "low_unique_code_ratio", metrics
    if metrics.get("key_fields_rate", 0.0) < config.AUTO_KEY_FIELDS_RATE_MIN:
        return False, "low_key_fields_rate", metrics

    return True, "passed", metrics


def apply_report_metrics(triage_result: TriageResult, report) -> None:
    total_rows = len(report.rows)
    triage_result.rows_exported = int(report.rows_exported or 0)
    triage_result.review_rows = int(report.rows_needs_review or 0)
    triage_result.review_rate = (
        triage_result.review_rows / total_rows if total_rows else 0.0
    )
    triage_result.rows_skipped_missing_target_currency = int(
        report.skipped_missing_target_price or 0
    )
    triage_result.duplicate_art_no_count = int(report.duplicate_art_no_count or 0)
    triage_result.duplicate_conflicts_count = int(
        report.duplicate_conflicts_count or 0
    )
    triage_result.bad_art_no_count = int(report.bad_art_no_count or 0)
    triage_result.corrected_art_no_count = int(report.corrected_art_no_count or 0)
    triage_result.suspicious_numeric_art_no_seen = bool(
        report.suspicious_numeric_art_no_seen
    )
    triage_result.examples_bad_art_no = report.examples_bad_art_no or []


def compute_unique_code_ratio(rows) -> float:
    codes = [text_utils.canonicalize_art_no(row.art_no) for row in rows if row.art_no]
    if not rows:
        return 0.0
    if not codes:
        return 0.0
    return len(set(codes)) / len(rows)


def compute_key_fields_rate(rows, currency: str) -> float:
    if not rows:
        return 0.0
    key_fields = 0
    for row in rows:
        if row.art_no and get_price_by_currency(row, currency) is not None:
            key_fields += 1
    return key_fields / len(rows)


def compute_review_rate_from_rows(rows) -> float:
    if not rows:
        return 1.0
    needs_review = sum(1 for row in rows if row.needs_review)
    return needs_review / len(rows)


def get_price_by_currency(row, currency: str):
    currency = currency.upper()
    if currency == "DKK":
        return row.price_dkk
    if currency == "SEK":
        return row.price_sek
    if currency == "NOK":
        return row.price_nok
    return row.price_eur


def format_attempt(
    parser_name: str,
    status: str,
    reason: str,
    metrics: dict,
    score: Optional[float] = None,
) -> str:
    parts = [f"{parser_name}:{status}"]
    if reason:
        parts.append(reason)
    if metrics:
        metrics_parts = []
        for key, value in metrics.items():
            if isinstance(value, float):
                metrics_parts.append(f"{key}={value:.2f}")
            else:
                metrics_parts.append(f"{key}={value}")
        if metrics_parts:
            parts.append(",".join(metrics_parts))
    if score is not None and "eval_score" not in (metrics or {}):
        parts.append(f"score={score:.3f}")
    return "(" + "|".join(parts) + ")"


def extract_last_failure(attempt: str) -> str:
    if not attempt:
        return ""
    stripped = attempt.strip("()")
    parts = stripped.split("|")
    if len(parts) >= 2:
        return parts[1]
    return ""


def compute_review_rate(report) -> float:
    return compute_review_rate_from_rows(report.rows)


def compute_duplicate_conflicts_rate(report, rows_exported: int) -> float:
    if rows_exported == 0:
        return 0.0
    return report.duplicate_conflicts_count / rows_exported


def build_basic_metrics(rows, currency: str) -> dict:
    exported = [row for row in rows if row.exported]
    rows_exported = len(exported)
    return {
        "rows_exported": rows_exported,
        "unique_code_ratio": compute_unique_code_ratio(exported),
        "key_fields_rate": compute_key_fields_rate(exported, currency),
        "review_rate": compute_review_rate_from_rows(rows),
        "duplicate_conflicts_rate": 0.0,
        "duplicate_conflicts_count": 0,
    }


def build_metrics_from_rows(rows, currency: str) -> dict:
    exported = [row for row in rows if row.exported]
    rows_exported = len(exported)
    duplicate_summary = pipeline.analyze_duplicates(rows, currency)
    duplicate_conflicts_count = duplicate_summary.get("conflicts_count", 0)
    duplicate_conflicts_rate = (
        duplicate_conflicts_count / rows_exported if rows_exported else 0.0
    )
    return {
        "rows_exported": rows_exported,
        "unique_code_ratio": compute_unique_code_ratio(exported),
        "key_fields_rate": compute_key_fields_rate(exported, currency),
        "review_rate": compute_review_rate_from_rows(rows),
        "duplicate_conflicts_rate": duplicate_conflicts_rate,
        "duplicate_conflicts_count": duplicate_conflicts_count,
    }


def evaluate_parser_fast(
    cached_pages: List[page_cache.CachedPage],
    parser_name: str,
    source_file: str,
    currency: str,
    currency_only: Optional[str],
    triage_result: Optional[TriageResult] = None,
) -> dict:
    start_time = time.monotonic()
    max_seconds = config.AUTO_MAX_SECONDS_PER_PARSER_EVAL
    target_currency = (currency or config.TARGET_CURRENCY).upper()
    filter_currency = bool(currency_only)
    parser = get_parser(parser_name)

    if parser_name == "table_based":
        ok, reason = table_precheck(parser, cached_pages, triage_result)
        if not ok:
            metrics = {
                "eval_pages_used": 0,
                "eval_rows": 0,
                "eval_time_ms": int((time.monotonic() - start_time) * 1000),
            }
            metrics["eval_score"] = score_run(metrics)
            return {
                "parser": parser_name,
                "ok": False,
                "reason": reason,
                "metrics": metrics,
                "score": metrics["eval_score"],
            }

    rows = []
    blocks = []
    current_section = ""
    processed_pages = 0
    block_index = 0
    sorted_pages = _sort_eval_pages(cached_pages)
    weak_signals = all(
        getattr(page, "cooccurrence_count", 0) == 0 for page in cached_pages
    )
    min_pages_before_discard = (
        config.AUTO_EVAL_MIN_PAGES_WEAK if weak_signals else 2
    )

    toc_hard_pages = [
        page
        for page in cached_pages
        if pipeline.is_toc_hard_page(
            page.normalized_text or "",
            mixed_code_count=int(getattr(page, "mixed_code_count", 0) or 0),
            row_candidate_count=int(getattr(page, "row_candidate_count", 0) or 0),
        )
    ]
    allow_toc_pages = len(toc_hard_pages) == len(cached_pages)

    for page in sorted_pages:
        elapsed = time.monotonic() - start_time
        if elapsed >= max_seconds:
            metrics = build_basic_metrics(rows, target_currency)
            metrics.update(
                {
                    "eval_pages_used": processed_pages,
                    "eval_rows": metrics.get("rows_exported", 0),
                    "eval_time_ms": int(elapsed * 1000),
                }
            )
            metrics["eval_score"] = score_run(metrics)
            return {
                "parser": parser_name,
                "ok": False,
                "reason": "eval_timeout",
                "metrics": metrics,
                "score": metrics["eval_score"],
            }

        if not page.normalized_text:
            continue
        if (
            not allow_toc_pages
            and pipeline.is_toc_hard_page(
                page.normalized_text or "",
                mixed_code_count=int(getattr(page, "mixed_code_count", 0) or 0),
                row_candidate_count=int(getattr(page, "row_candidate_count", 0) or 0),
            )
        ):
            continue
        processed_pages += 1
        lines = page.lines
        if not lines:
            continue
        section = parser.detect_section(lines)
        if section:
            current_section = section
            lines = normalize.strip_section_line(lines, section)

        page_blocks = parser.segment_blocks(lines)
        for block_text in page_blocks:
            notes = []
            has_art_no = parser.contains_art_no(block_text)
            has_price = bool(parser.price_pattern.search(block_text))
            if not has_art_no and has_price:
                if blocks and blocks[-1]["page"] in {
                    page.page_number,
                    page.page_number - 1,
                }:
                    blocks[-1]["raw_text"] += "\n" + block_text
                    blocks[-1]["notes"].append("continuation_attached")
                    continue
                notes.append("orphan_block_no_art_no")
            blocks.append(
                {
                    "page": page.page_number,
                    "section": current_section,
                    "raw_text": block_text,
                    "ocr_used": page.ocr_used,
                    "notes": notes,
                }
            )

        for block in blocks[block_index:]:
            sub_blocks, merged_note = parser.split_merged_block(block["raw_text"])
            for sub_block in sub_blocks:
                row, size_unparsed, parse_notes = parser.parse_block(
                    raw_text=sub_block,
                    page=block["page"],
                    section=block["section"],
                    source_file=source_file,
                )
                normalized_block = normalize.normalize_text(sub_block)
                flat_line = " ".join(normalized_block.split())
                line_info = text_utils.analyze_line(flat_line)
                token_info = text_utils.resolve_row_fields(flat_line)
                selected_art_no = token_info.get("selected_art_no") or {}
                if not row.art_no and selected_art_no:
                    art_no_value = selected_art_no.get("token") if isinstance(selected_art_no, dict) else ""
                    if art_no_value and not token_info.get("ambiguous_numeric"):
                        row.art_no = art_no_value
                        row.art_no_raw = art_no_value
                if not row.product_name_en and token_info.get("name"):
                    row.product_name_en = token_info.get("name") or ""
                if not row.size_raw and token_info.get("dimension_candidates"):
                    row.size_raw = " | ".join(token_info.get("dimension_candidates") or [])
                discard_reason = pipeline.primary_discard_reason(
                    parser=parser,
                    row=row,
                    raw_text=flat_line,
                    line_info=line_info,
                    token_info=token_info,
                    export_policy_mode=config.EXPORT_POLICY_MODE,
                )
                if discard_reason:
                    if discard_reason == "bad_id" and (
                        token_info.get("ambiguous_numeric")
                        or token_info.get("art_no_candidates")
                        or token_info.get("price_candidates")
                    ):
                        discard_reason = ""
                    if discard_reason:
                        continue
                confidence, needs_review, notes = scoring.score_row(
                    row=row,
                    size_unparsed=size_unparsed,
                    ocr_used=block.get("ocr_used", False),
                    currency=target_currency,
                )

                row.confidence = confidence
                row.needs_review = needs_review

                combined_notes = parse_notes + notes + block.get("notes", [])
                if merged_note:
                    combined_notes.append(merged_note)
                if token_info.get("dimension_candidates"):
                    combined_notes.append("dim_detected")
                if token_info.get("price_candidates"):
                    combined_notes.append("price_detected")
                if token_info.get("art_no_candidates"):
                    combined_notes.append("artno_detected")
                if token_info.get("ambiguous_numeric"):
                    row.exported = False
                    row.needs_review = True
                    combined_notes.append("ambiguous_price_vs_artno")

                if row.art_no:
                    row.art_no = text_utils.canonicalize_art_no(row.art_no)
                valid_art_no = text_utils.is_valid_art_no_token(
                    row.art_no or "", min_len=config.CODE_MIN_LEN
                )
                row.raw_snippet = flat_line[:200]

                invalid_price_note = f"invalid_price_{target_currency.lower()}"
                force_review = merged_note in {
                    "merged_block_unsplit",
                    "possible_merged_block",
                }
                if invalid_price_note in parse_notes:
                    force_review = True

                art_no_count = len(parser.art_no_regex.findall(sub_block))
                if parser.name in {"table_based", "code_price_based"}:
                    art_no_count = 1 if row.art_no else 0
                has_any_price = bool(parser.price_pattern.search(sub_block))
                price_regex = getattr(parser, "price_regex", None)
                if price_regex and price_regex.search(sub_block):
                    has_any_price = True
                if line_info.get("price_like"):
                    has_any_price = True
                if pipeline.row_has_price(row):
                    has_any_price = True
                invalid_reasons = []
                if not valid_art_no:
                    invalid_reasons.append("invalid_chunk_missing_art_no")
                elif art_no_count > 1 and parser.name not in {"table_based", "code_price_based"}:
                    invalid_reasons.append("invalid_chunk_multiple_art_no")
                if not has_any_price:
                    invalid_reasons.append("invalid_chunk_missing_price")
                if not row.product_name_en:
                    invalid_reasons.append("invalid_chunk_missing_name")
                if not token_info.get("price_candidates") and not token_info.get("art_no_candidates"):
                    invalid_reasons.append("no_price_no_artno")
                if invalid_reasons:
                    row.exported = False
                    row.needs_review = True
                    combined_notes.extend(invalid_reasons)

                strong_signal = bool(
                    pipeline.row_has_price(row)
                    or line_info.get("price_like")
                    or token_info.get("price_candidates")
                )
                hard_ok, hard_reason = pipeline.hard_validate_row(
                    row=row,
                    raw_text=flat_line,
                    line_info=line_info,
                    token_info=token_info,
                    export_policy_mode=config.EXPORT_POLICY_MODE,
                    strong_signal=strong_signal,
                )
                if not hard_ok:
                    row.exported = False
                    row.needs_review = True
                    combined_notes.append(hard_reason)

                if pipeline.is_degraded_art_no(sub_block, row.art_no):
                    combined_notes.append("art_no_degraded")
                    row.needs_review = True

                row.notes = "; ".join(pipeline.unique_notes(combined_notes))
                if force_review:
                    row.needs_review = True

                if filter_currency:
                    pipeline.apply_currency_filter(row, target_currency)
                    if pipeline.get_price_by_currency(row, target_currency) is None:
                        continue

                rows.append(row)
        block_index = len(blocks)

        early_reason = early_discard_reason(
            rows,
            target_currency,
            processed_pages,
            min_pages=min_pages_before_discard,
        )
        if early_reason:
            metrics = build_basic_metrics(rows, target_currency)
            metrics.update(
                {
                    "eval_pages_used": processed_pages,
                    "eval_rows": metrics.get("rows_exported", 0),
                    "eval_time_ms": int((time.monotonic() - start_time) * 1000),
                }
            )
            metrics["eval_score"] = score_run(metrics)
            return {
                "parser": parser_name,
                "ok": False,
                "reason": early_reason,
                "metrics": metrics,
                "score": metrics["eval_score"],
            }

    pipeline.apply_duplicate_policy(rows, target_currency, {})
    pipeline.ensure_review_for_missing_prices(rows, target_currency)
    metrics = build_metrics_from_rows(rows, target_currency)
    metrics.update(
        {
            "eval_pages_used": processed_pages,
            "eval_rows": metrics.get("rows_exported", 0),
            "eval_time_ms": int((time.monotonic() - start_time) * 1000),
        }
    )
    ok, reason, metrics = evaluate_metrics(metrics)
    metrics["eval_score"] = score_run(metrics)
    return {
        "parser": parser_name,
        "ok": ok,
        "reason": reason,
        "metrics": metrics,
        "score": metrics["eval_score"],
    }


def early_discard_reason(
    rows,
    currency: str,
    pages_processed: int,
    min_pages: int = 2,
) -> str:
    if pages_processed < max(2, min_pages):
        return ""
    exported = [row for row in rows if row.exported]
    if not exported:
        return "early_discard_no_rows"
    if len(exported) < config.AUTO_ROWS_MIN:
        return ""
    unique_ratio = compute_unique_code_ratio(exported)
    key_fields_rate = compute_key_fields_rate(exported, currency)
    if unique_ratio < (config.AUTO_UNIQUE_CODE_RATIO_MIN * config.AUTO_EARLY_DISCARD_FACTOR):
        return "early_discard_low_unique"
    if key_fields_rate < (config.AUTO_KEY_FIELDS_RATE_MIN * config.AUTO_EARLY_DISCARD_FACTOR):
        return "early_discard_low_key_fields"
    return ""


def table_precheck(parser, cached_pages: List[page_cache.CachedPage], triage_result):
    if not cached_pages:
        return False, "no_eval_pages"
    page = next((item for item in cached_pages if item.table_hint), cached_pages[0])
    column_lines = 0
    header_found = False
    for line in page.lines:
        columns = parser._split_columns(line)
        if parser._detect_header_map(columns):
            header_found = True
            break
        if len(columns) >= config.TRIAGE_TABLE_MIN_COLUMNS:
            column_lines += 1

    if header_found:
        return True, ""
    if triage_result and triage_result.table_columns >= config.TRIAGE_TABLE_MIN_COLUMNS:
        return True, ""
    if column_lines >= 3:
        return True, ""
    return False, "table_precheck_failed"


def score_run(metrics: dict) -> float:
    key_fields_rate = metrics.get("key_fields_rate", 0.0) or 0.0
    unique_ratio = metrics.get("unique_code_ratio", 0.0) or 0.0
    review_rate = metrics.get("review_rate", 1.0) or 1.0
    duplicate_conflicts_rate = metrics.get("duplicate_conflicts_rate", 0.0) or 0.0
    return (
        config.AUTO_SCORE_WEIGHT_KEY_FIELDS * key_fields_rate
        + config.AUTO_SCORE_WEIGHT_UNIQUE * unique_ratio
        - config.AUTO_SCORE_WEIGHT_REVIEW * review_rate
        - config.AUTO_SCORE_WEIGHT_DUP_CONFLICT * duplicate_conflicts_rate
    )


def select_best_run(attempt_results: List[dict]) -> Optional[dict]:
    valid = [item for item in attempt_results if item.get("ok")]
    if not valid:
        return None
    valid.sort(
        key=lambda item: (
            item.get("score", 0.0),
            item.get("metrics", {}).get("rows_exported", 0),
            -(item.get("metrics", {}).get("review_rate", 1.0)),
        ),
        reverse=True,
    )
    return valid[0]


def finalize_selection(
    triage_result: TriageResult,
    selection: dict,
    attempts: List[str],
    output_dir: Path,
    stem: str,
) -> TriageResult:
    parser_name = selection.get("parser", "")
    metrics = selection.get("metrics", {}) or {}
    score = selection.get("score", 0.0)
    final_xlsx = output_dir / f"{stem}.xlsx"
    selected_path = output_dir / f"{stem}.{parser_name}.xlsx"
    if final_xlsx.exists():
        try:
            final_xlsx.unlink()
        except OSError:
            LOGGER.warning("Could not remove existing output %s", final_xlsx)
    if selected_path.exists():
        try:
            selected_path.rename(final_xlsx)
        except OSError:
            LOGGER.warning("Could not rename %s to %s", selected_path, final_xlsx)
    for profile_id, parser in PROFILE_PARSER_MAP.items():
        if parser == parser_name:
            continue
        extra_path = output_dir / f"{stem}.{parser}.xlsx"
        if extra_path.exists():
            try:
                extra_path.unlink()
            except OSError:
                LOGGER.warning("Could not remove extra output %s", extra_path)
    triage_result.final_status = f"CONVERTED(parser={parser_name})"
    triage_result.final_parser = parser_name
    triage_result.winner_parser = parser_name
    triage_result.output_path = str(final_xlsx)
    triage_result.attempts_count = len(attempts)
    triage_result.attempts_summary = "; ".join(attempts)
    triage_result.selection_reason = (
        f"selected_score={score:.3f}"
        f" key_fields_rate={metrics.get('key_fields_rate', 0):.2f}"
        f" unique_code_ratio={metrics.get('unique_code_ratio', 0):.2f}"
        f" review_rate={metrics.get('review_rate', 0):.2f}"
    )
    return triage_result


def is_excellent(metrics: dict) -> bool:
    return (
        metrics.get("key_fields_rate", 0.0) >= config.AUTO_EXCELLENT_KEY_FIELDS_RATE_MIN
        and metrics.get("review_rate", 1.0) <= config.AUTO_EXCELLENT_REVIEW_RATE_MAX
    )
