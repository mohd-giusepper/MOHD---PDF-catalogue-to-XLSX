from typing import Iterable, List

from openpyxl import Workbook

from pdf2xlsx.models import ProductRow
from pdf2xlsx.utils import text as text_utils


HEADERS: List[str] = [
    "source_file",
    "page",
    "section",
    "product_name_en",
    "product_name_raw",
    "variant",
    "designer",
    "art_no",
    "colli",
    "size_raw",
    "width_cm",
    "height_cm",
    "length_cm",
    "price_dkk",
    "price_sek",
    "price_nok",
    "price_eur",
    "barcode",
    "confidence",
    "needs_review",
    "notes",
]


def write_xlsx(rows: Iterable[ProductRow], output_path: str) -> None:
    workbook = Workbook()
    products_sheet = workbook.active
    products_sheet.title = "PRODUCTS"
    review_sheet = workbook.create_sheet("REVIEW")

    products_sheet.append(HEADERS)
    review_headers = [
        "source_file",
        "page",
        "section",
        "product_name_en",
        "variant",
        "designer",
        "art_no",
        "price_eur",
        "needs_review",
        "exported",
        "notes",
        "raw_block_id",
        "raw_snippet",
    ]
    review_sheet.append(review_headers)

    sorted_rows = sorted(rows, key=_sort_key)
    for row in sorted_rows:
        row_dict = row.to_dict()
        if row.exported:
            products_sheet.append([row_dict.get(header) for header in HEADERS])
        if row.needs_review or not row.exported:
            review_sheet.append([row_dict.get(header) for header in review_headers])

    workbook.save(output_path)


def _sort_key(row: ProductRow):
    page = row.page if row.page is not None else 0
    section = (row.section or "").lower()
    art_no = text_utils.canonicalize_art_no(row.art_no)
    return (page, section, art_no)
