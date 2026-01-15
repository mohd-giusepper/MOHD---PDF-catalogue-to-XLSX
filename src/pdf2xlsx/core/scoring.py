from typing import List, Tuple

from pdf2xlsx import config
from pdf2xlsx.models import ProductRow


def score_row(
    row: ProductRow,
    size_unparsed: bool = False,
    ocr_used: bool = False,
    threshold: float = config.CONFIDENCE_THRESHOLD,
) -> Tuple[float, bool, List[str]]:
    confidence = 1.0
    notes: List[str] = []

    if ocr_used:
        confidence -= config.OCR_CONFIDENCE_PENALTY
        notes.append("ocr_used")
    if not row.art_no:
        confidence -= 0.8
        notes.append("missing_art_no")
    if not row.product_name_en:
        confidence -= 0.4
        notes.append("missing_product_name")
    if row.price_eur is None:
        confidence -= 0.2
        notes.append("missing_price_eur")
    if row.size_raw and size_unparsed:
        confidence -= 0.1
        notes.append("size_unparsed")

    confidence = max(0.0, min(confidence, 1.0))
    needs_review = confidence < threshold or not row.art_no or not row.product_name_en

    if ocr_used and (not row.art_no or _missing_all_prices(row)):
        needs_review = True
        notes.append("ocr_missing_key_fields")

    return confidence, needs_review, notes


def _missing_all_prices(row: ProductRow) -> bool:
    return (
        row.price_dkk is None
        and row.price_sek is None
        and row.price_nok is None
        and row.price_eur is None
    )
