import hashlib
import json
import re
from pathlib import Path
from typing import Dict, List, Optional

import pdfplumber

from pdf2xlsx import config
from pdf2xlsx.core import page_cache, triage
from pdf2xlsx.core import suggestions as suggestions_utils
from pdf2xlsx.utils import labels as label_utils


CURRENCY_TOKEN_RE = re.compile(r"\b[A-Z]{3}\b")
KNOWN_CURRENCIES = {"EUR", "DKK", "SEK", "NOK", "USD", "GBP", "CHF", "JPY", "CAD", "AUD"}


def write_debug_json(
    pdf_path: str,
    triage_result,
    cached_pages: List[page_cache.CachedPage],
    output_dir: str,
    report: Optional[object] = None,
    reason: str = "",
    force: bool = False,
) -> Optional[str]:
    payload = build_debug_payload(
        pdf_path=pdf_path,
        triage_result=triage_result,
        cached_pages=cached_pages,
        report=report,
        reason=reason,
    )
    output_path = Path(output_dir) / f"{Path(pdf_path).stem}{config.DEBUG_JSON_SUFFIX}"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)
    return str(output_path)


def build_debug_payload(
    pdf_path: str,
    triage_result,
    cached_pages: List[page_cache.CachedPage],
    report: Optional[object] = None,
    reason: str = "",
) -> dict:
    label_dict = label_utils.load_label_dictionary()
    base_patterns = label_utils.build_label_patterns(label_dict.get("fields", {}))
    normalized_text = "\n".join(page.normalized_text or "" for page in cached_pages)
    size_hits = label_utils.count_label_hits(base_patterns, "size", normalized_text)
    digits_count = sum(1 for char in normalized_text if char.isdigit())
    numeric_density = digits_count / len(normalized_text) if normalized_text else 0.0
    currency_tokens = collect_currency_tokens("\n".join(page.text or "" for page in cached_pages))
    table_columns, numeric_ratio = compute_table_metrics(cached_pages)
    table_likelihood = table_columns + numeric_ratio * 5.0
    page_count = get_page_count(pdf_path)
    pdf_hash = compute_pdf_hash(pdf_path)

    summary = {
        "pdf_path": pdf_path,
        "pdf_hash": pdf_hash,
        "page_count": page_count,
        "decision": triage_result.decision,
        "selected_parser": triage_result.winner_parser
        or triage_result.final_parser
        or triage_result.suggested_profile
        or "",
        "final_status": triage_result.final_status or "",
        "reason": reason or triage_result.failure_reason or triage_result.selection_reason or triage_result.reasons,
    }

    triage_signals = {
        "marker_counts": {
            "art_no": triage_result.art_no_count,
            "code": triage_result.code_label_count,
            "rrp": triage_result.rrp_count,
            "size": size_hits,
        },
        "currency_tokens": currency_tokens,
        "numeric_density": round(numeric_density, 4),
        "table_likelihood": {
            "columns": table_columns,
            "numeric_ratio": round(numeric_ratio, 3),
            "score": round(table_likelihood, 2),
        },
    }

    sampling = {
        "pages_scanned_budget": config.TRIAGE_SAMPLE_PAGES_MAX,
        "pages_sampled": triage_result.pages_sampled or [],
        "page_metrics": build_page_metrics(cached_pages),
        "cached_pages_source": getattr(triage_result, "cached_pages_source", ""),
        "sampling_retry_triggered": bool(
            getattr(triage_result, "sampling_retry_triggered", False)
        ),
        "sampling_retry_reason": getattr(triage_result, "sampling_retry_reason", ""),
        "sampling_retry_count": int(
            getattr(triage_result, "sampling_retry_count", 0) or 0
        ),
        "sampling_retry_old_sample_count": int(
            getattr(triage_result, "sampling_retry_old_sample_count", 0) or 0
        ),
        "sampling_retry_new_sample_count": int(
            getattr(triage_result, "sampling_retry_new_sample_count", 0) or 0
        ),
        "sampling_retry_pages_sampled_old": getattr(
            triage_result, "sampling_retry_pages_sampled_old", []
        )
        or [],
        "sampling_retry_pages_sampled_new": getattr(
            triage_result, "sampling_retry_pages_sampled_new", []
        )
        or [],
        "toc_like_pages_candidate": int(
            getattr(triage_result, "toc_like_pages_candidate", 0) or 0
        ),
        "toc_hard_pages_candidate": int(
            getattr(triage_result, "toc_hard_pages_candidate", 0) or 0
        ),
        "toc_hard_pages_excluded": int(
            getattr(triage_result, "toc_hard_pages_excluded", 0) or 0
        ),
        "top_k_min_target": int(getattr(triage_result, "top_k_min_target", 0) or 0),
        "top_k_reintegrated": bool(
            getattr(triage_result, "top_k_reintegrated", False)
        ),
        "top_k_reintegrated_count": int(
            getattr(triage_result, "top_k_reintegrated_count", 0) or 0
        ),
        "top_k_collapse_reason": getattr(triage_result, "top_k_collapse_reason", ""),
    }

    attempts = format_attempts(triage_result.attempts_detail or [])

    rows_total = len(getattr(report, "rows", []) or []) if report else 0
    output_metrics = {
        "rows_total": rows_total,
        "rows_exported": triage_result.rows_exported or 0,
        "review_rows": triage_result.review_rows or 0,
        "rows_review": triage_result.review_rows or 0,
        "rows_noise": getattr(report, "guardrail_counts", {}).get("noise_rows", 0)
        if report
        else 0,
        "review_rate": triage_result.review_rate or 0.0,
        "duplicates_count": triage_result.duplicate_conflicts_count or 0,
        "skipped_missing_currency_count": triage_result.rows_skipped_missing_target_currency or 0,
    }

    payload = {
        "summary": summary,
        "triage_signals": triage_signals,
        "sampling": sampling,
        "attempts": attempts,
        "output_metrics": output_metrics,
    }
    if report:
        payload["analysis"] = {
            "rows_candidate": getattr(report, "rows_candidate", 0),
            "rows_after_parsing": getattr(report, "rows_after_parsing", 0),
            "rows_after_filters": getattr(report, "rows_after_filters", 0),
            "discard_reasons": getattr(report, "discard_reasons", {}) or {},
            "discard_samples": getattr(report, "discard_samples", {}) or {},
            "guardrail_counts": getattr(report, "guardrail_counts", {}) or {},
            "page_skip_reasons": getattr(report, "page_skip_reasons", {}) or {},
            "duplicates_summary": getattr(report, "duplicates_summary", []) or [],
            "cooccurrence_samples": getattr(report, "cooccurrence_samples", []) or [],
            "export_policy_mode": (getattr(report, "config_info", {}) or {}).get(
                "export_policy_mode", ""
            ),
        }

    if _needs_suggestions(triage_result):
        suggestions = suggestions_utils.build_suggestions(
            pdf_path=pdf_path,
            cached_pages=cached_pages,
            reason=triage_result.reasons or "triage",
        )
        payload["suggestions"] = {
            "label_candidates": collect_label_candidates(cached_pages),
            "code_pattern_candidates": suggestions.get("code_candidates", [])[:5],
            "price_format_candidates": suggestions.get("price_patterns", {}),
            "recommended_parser": map_profile_to_strategy(
                triage_result.suggested_profile
            ),
        }

    return payload


def map_profile_to_strategy(profile: str) -> str:
    if profile == "stelton_marker":
        return "marker_based"
    if profile == "table_based":
        return "table_based"
    if profile == "code_price_based":
        return "code_price_based"
    return "unknown"


def compute_pdf_hash(pdf_path: str) -> str:
    hasher = hashlib.sha1()
    with Path(pdf_path).open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()[:8]


def get_page_count(pdf_path: str) -> int:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except Exception:
        return 0


def collect_currency_tokens(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    if not text:
        return counts
    euro_symbol = text.count("\u20ac")
    if euro_symbol:
        counts["EUR_symbol"] = euro_symbol
    for token in CURRENCY_TOKEN_RE.findall(text):
        if token in KNOWN_CURRENCIES:
            counts[token] = counts.get(token, 0) + 1
    return counts


def compute_table_metrics(cached_pages: List[page_cache.CachedPage]) -> tuple:
    table_columns = 0
    numeric_ratios: List[float] = []
    for page in cached_pages:
        columns, ratio = triage.table_metrics_from_words(page.words)
        table_columns = max(table_columns, columns)
        numeric_ratios.append(ratio)
    numeric_ratio = sum(numeric_ratios) / len(numeric_ratios) if numeric_ratios else 0.0
    return table_columns, numeric_ratio


def _needs_suggestions(triage_result) -> bool:
    if triage_result.decision in {"FORSE", "NO"}:
        return True
    if (triage_result.final_status or "").startswith("FAILED"):
        return True
    return False


def _should_write_debug(triage_result) -> bool:
    return (triage_result.final_status or "").startswith("FAILED")


def build_page_metrics(cached_pages: List[page_cache.CachedPage]) -> List[dict]:
    metrics = []
    for page in cached_pages:
        metrics.append(
            {
                "page": page.page_number,
                "text_len": page.text_len,
                "numeric_density": page.numeric_density,
                "currency_tokens": page.currency_tokens,
                "table_likelihood": page.table_likelihood,
                "mixed_code_count": page.mixed_code_count,
                "price_like_count": page.price_like_count,
                "cooccurrence_count": page.cooccurrence_count,
                "cooccurrence_near_count": getattr(page, "cooccurrence_near_count", 0),
                "row_candidate_count": getattr(page, "row_candidate_count", 0),
                "toc_like": bool(getattr(page, "toc_like", False)),
                "toc_hard": bool(getattr(page, "toc_hard", False)),
                "toc_score": int(getattr(page, "toc_score", 0) or 0),
            }
        )
    return metrics


def format_attempts(attempts: List[dict]) -> List[dict]:
    output = []
    for attempt in attempts:
        output.append(
            {
                "parser": attempt.get("parser", ""),
                "eval_score": attempt.get("eval_score", 0.0),
                "eval_rows": attempt.get("eval_rows", 0),
                "eval_time_ms": attempt.get("eval_time_ms", 0),
                "status": attempt.get("status", ""),
                "fail_reason": attempt.get("fail_reason", ""),
            }
        )
    return output


def collect_label_candidates(
    cached_pages: List[page_cache.CachedPage],
    max_items: int = 30,
    min_count: int = 3,
) -> List[dict]:
    counter: Dict[str, int] = {}
    for page in cached_pages:
        for line in page.lines:
            clean = (line or "").strip()
            if not clean or len(clean) > 120:
                continue
            if triage.PRICE_RE.search(clean):
                continue
            if not _is_label_like(clean):
                continue
            counter[clean] = counter.get(clean, 0) + 1

    items = []
    for line, count in counter.items():
        if count < min_count:
            continue
        if len(line.split()) > 6:
            continue
        items.append((line, count))

    items.sort(key=lambda item: (-item[1], item[0]))
    return [{"line": line, "count": count} for line, count in items[:max_items]]


def _is_label_like(line: str) -> bool:
    if line.endswith(":"):
        return True
    if "  " in line or "\t" in line:
        return True
    letters = [char for char in line if char.isalpha()]
    if letters:
        upper_ratio = sum(1 for char in letters if char.isupper()) / len(letters)
        if upper_ratio >= 0.6 and len(line) <= 40:
            return True
    return False
