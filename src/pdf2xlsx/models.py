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
    noise: bool = False

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
    skipped_missing_target_price: int = 0
    rows_exported: int = 0
    duplicate_art_no_count: int = 0
    duplicate_art_no_top: List[tuple] = field(default_factory=list)
    duplicate_conflicts: List[str] = field(default_factory=list)
    duplicate_conflicts_count: int = 0
    bad_art_no_count: int = 0
    corrected_art_no_count: int = 0
    suspicious_numeric_art_no_seen: bool = False
    examples_bad_art_no: List[dict] = field(default_factory=list)
    review_reasons_top: List[tuple] = field(default_factory=list)
    target_currency: str = "EUR"
    examples_ok: List[ProductRow] = field(default_factory=list)
    examples_needs_review: List[ProductRow] = field(default_factory=list)
    page_stats: List[dict] = field(default_factory=list)
    rows_candidate: int = 0
    rows_after_parsing: int = 0
    rows_after_filters: int = 0
    discard_reasons: dict = field(default_factory=dict)
    discard_samples: dict = field(default_factory=dict)
    duplicates_summary: List[dict] = field(default_factory=list)
    cooccurrence_samples: List[str] = field(default_factory=list)
    guardrail_counts: dict = field(default_factory=dict)
    page_skip_reasons: dict = field(default_factory=dict)
    config_info: dict = field(default_factory=dict)


@dataclass
class TriageResult:
    source_file: str = ""
    source_path: str = ""
    pages_sampled: List[int] = field(default_factory=list)
    suggested_profile: str = ""
    support_score: float = 0.0
    decision: str = ""
    parser: str = ""
    marker_score: float = 0.0
    table_score: float = 0.0
    code_price_score: float = 0.0
    art_no_count: int = 0
    rrp_count: int = 0
    colli_count: int = 0
    designer_count: int = 0
    code_label_count: int = 0
    description_label_count: int = 0
    price_label_count: int = 0
    price_count: int = 0
    euro_count: int = 0
    currency_code_count: int = 0
    table_columns: int = 0
    numeric_line_ratio: float = 0.0
    text_len_total: int = 0
    ocr_needed_pages: int = 0
    ocr_used_pages: int = 0
    reasons: str = ""
    target_currency: str = ""
    currency_confidence: float = 0.0
    currency_counts: dict = field(default_factory=dict)
    final_status: str = ""
    final_parser: str = ""
    winner_parser: str = ""
    eval_time_ms_total: int = 0
    output_path: str = ""
    rows_exported: int = 0
    review_rows: int = 0
    review_rate: float = 0.0
    rows_skipped_missing_target_currency: int = 0
    duplicate_art_no_count: int = 0
    duplicate_conflicts_count: int = 0
    bad_art_no_count: int = 0
    corrected_art_no_count: int = 0
    suspicious_numeric_art_no_seen: bool = False
    examples_bad_art_no: List[dict] = field(default_factory=list)
    attempts_count: int = 0
    attempts_summary: str = ""
    attempts_detail: List[dict] = field(default_factory=list)
    selection_reason: str = ""
    failure_reason: str = ""
    cached_pages_source: str = ""
    sampling_retry_triggered: bool = False
    sampling_retry_reason: str = ""
    sampling_retry_count: int = 0
    sampling_retry_old_sample_count: int = 0
    sampling_retry_new_sample_count: int = 0
    sampling_retry_pages_sampled_old: List[int] = field(default_factory=list)
    sampling_retry_pages_sampled_new: List[int] = field(default_factory=list)
    toc_like_pages_candidate: int = 0
    toc_hard_pages_candidate: int = 0
    toc_hard_pages_excluded: int = 0
    top_k_min_target: int = 0
    top_k_reintegrated: bool = False
    top_k_reintegrated_count: int = 0
    top_k_collapse_reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)
