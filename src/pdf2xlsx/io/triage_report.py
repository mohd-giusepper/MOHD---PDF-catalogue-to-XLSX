from typing import Iterable, List

from openpyxl import Workbook

from pdf2xlsx.models import TriageResult


HEADERS: List[str] = [
    "source_file",
    "suggested_profile",
    "support_score",
    "decision",
    "parser",
    "reasons",
    "final_status",
    "final_parser",
    "winner_parser",
    "eval_time_ms_total",
    "output_path",
    "target_currency",
    "rows_exported",
    "review_rows",
    "review_rate",
    "rows_skipped_missing_target_currency",
    "duplicate_art_no_count",
    "duplicate_conflicts_count",
    "bad_art_no_count",
    "corrected_art_no_count",
    "suspicious_numeric_art_no_seen",
    "examples_bad_art_no",
    "attempts_count",
    "attempts_summary",
    "selection_reason",
    "failure_reason",
    "pages_sampled",
    "marker_score",
    "table_score",
    "code_price_score",
    "art_no_count",
    "rrp_count",
    "colli_count",
    "designer_count",
    "code_label_count",
    "description_label_count",
    "price_label_count",
    "price_count",
    "euro_count",
    "currency_code_count",
    "table_columns",
    "numeric_line_ratio",
    "text_len_total",
    "ocr_needed_pages",
    "ocr_used_pages",
]


def write_triage_report(results: Iterable[TriageResult], output_path: str) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "TRIAGE"
    sheet.append(HEADERS)

    attempts_sheet = workbook.create_sheet("ATTEMPTS")
    attempts_headers = [
        "source_file",
        "parser",
        "eval_pages",
        "eval_rows",
        "eval_time_ms",
        "eval_score",
        "status",
        "fail_reason",
    ]
    attempts_sheet.append(attempts_headers)

    for result in results:
        row = result.to_dict()
        row["pages_sampled"] = ",".join(
            str(page) for page in row.get("pages_sampled") or []
        )
        for key in ["examples_bad_art_no", "currency_counts"]:
            if key in row and row[key] is not None:
                row[key] = str(row[key])
        sheet.append([row.get(header) for header in HEADERS])
        for attempt in row.get("attempts_detail") or []:
            attempts_sheet.append([attempt.get(header) for header in attempts_headers])

    workbook.save(output_path)
