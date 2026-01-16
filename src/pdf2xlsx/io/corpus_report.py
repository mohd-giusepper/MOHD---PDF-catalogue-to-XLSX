from typing import Iterable, List

from openpyxl import Workbook


HEADERS: List[str] = [
    "path",
    "page_count",
    "pages_sampled",
    "marker_hits",
    "currency_tokens",
    "numeric_density",
    "price_pattern_hits",
    "table_likelihood",
    "recommended_parser",
    "risk_flags",
]


def write_corpus_report(rows: Iterable[dict], output_path: str) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "CORPUS"
    sheet.append(HEADERS)

    for row in rows:
        row_out = dict(row)
        pages = row_out.get("pages_sampled") or []
        row_out["pages_sampled"] = ",".join(str(page) for page in pages)
        sheet.append([row_out.get(header) for header in HEADERS])

    workbook.save(output_path)
