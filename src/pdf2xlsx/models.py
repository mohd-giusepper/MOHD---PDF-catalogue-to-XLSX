from dataclasses import asdict, dataclass, field
from typing import List, Optional


@dataclass
class ProductRow:
    source_file: str = ""
    page: Optional[int] = None
    section: str = ""
    product_name_en: str = ""
    product_name_raw: str = ""
    variant: str = ""
    designer: str = ""
    art_no: str = ""
    art_no_raw: str = ""
    colli: Optional[int] = None
    size_raw: str = ""
    width_cm: Optional[float] = None
    height_cm: Optional[float] = None
    length_cm: Optional[float] = None
    price_dkk: Optional[float] = None
    price_sek: Optional[float] = None
    price_nok: Optional[float] = None
    price_eur: Optional[float] = None
    barcode: str = ""
    confidence: float = 0.0
    needs_review: bool = True
    notes: str = ""
    exported: bool = True
    raw_block_id: str = ""
    raw_snippet: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RunReport:
    rows: List[ProductRow] = field(default_factory=list)
    pages_processed: int = 0
    pages_needing_ocr: int = 0
    pages_ocr_used: int = 0
    rows_needs_review: int = 0
    missing_art_no: int = 0
    missing_price: int = 0
    rows_exported: int = 0
    duplicate_art_no_count: int = 0
    duplicate_art_no_top: List[tuple] = field(default_factory=list)
    duplicate_conflicts: List[str] = field(default_factory=list)
    duplicate_conflicts_count: int = 0
    review_reasons_top: List[tuple] = field(default_factory=list)
    target_currency: str = "EUR"
    examples_ok: List[ProductRow] = field(default_factory=list)
    examples_needs_review: List[ProductRow] = field(default_factory=list)
    page_stats: List[dict] = field(default_factory=list)
    config_info: dict = field(default_factory=dict)
