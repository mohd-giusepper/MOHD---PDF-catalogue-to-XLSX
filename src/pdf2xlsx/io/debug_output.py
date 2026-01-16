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
    }

    attempts = format_attempts(triage_result.attempts_detail or [])

    rows_total = len(getattr(report, "rows", []) or []) if report else 0
    output_metrics = {
        "rows_total": rows_total,
        "rows_exported": triage_result.rows_exported or 0,
        "review_rows": triage_result.review_rows or 0,
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

    items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return [{"line": line, "count": count} for line, count in items[:max_items]]


def _is_label_like(line: str) -> bool:
    if line.endswith(":"):
        return True
    if "  " in line or "\t" in line:
        return True
    return False
