import re
from typing import Dict, List, Optional, Tuple

from pdf2xlsx.models import ProductRow
from pdf2xlsx.parsers.base import BaseParser
from pdf2xlsx.utils import labels as label_utils
from pdf2xlsx.utils import text as text_utils


class TableBasedParser(BaseParser):
    name = "table_based"
    column_split_regex = re.compile(r"\s{2,}|\t")
    price_regex = re.compile(r"(?:€\s*)?([0-9]{1,7}(?:[.,][0-9]{2})?)")

    def __init__(self) -> None:
        profile_dict = label_utils.load_profile_dictionary("table_based")
        self.label_patterns = label_utils.build_label_patterns(
            profile_dict.get("fields", {})
        )
        self.header_map: Dict[str, int] = {}

    def segment_blocks(self, lines: List[str]) -> List[str]:
        blocks = []
        for line in lines:
            columns = self._split_columns(line)
            if len(columns) < 2:
                continue
            header_map = self._detect_header_map(columns)
            if header_map:
                self.header_map = header_map
                continue
            if self._is_header_like(line):
                continue
            blocks.append("||".join(columns))
        return blocks

    def parse_block(
        self, raw_text: str, page: int, section: str, source_file: str
    ) -> Tuple[ProductRow, bool, List[str]]:
        columns = self._split_columns(raw_text.replace("||", "  "))
        code = ""
        code_raw = ""
        description = ""
        price_value = None

        if self.header_map:
            code = self._get_column(columns, "code")
            description = self._get_column(columns, "description")
            price_value = self._parse_price_column(self._get_column(columns, "price"))
        if not code:
            code = columns[0] if columns else ""
        if not description and len(columns) > 2:
            description = " ".join(columns[1:-1])
        if price_value is None and columns:
            price_value = self._parse_price_column(columns[-1])

        code_raw = code
        code = self.validate_art_no(code, raw_value=code_raw)

        row = ProductRow(
            source_file=source_file,
            page=page,
            section=section or "",
            product_name_en=description or "",
            product_name_raw=raw_text.replace("||", " "),
            variant="",
            designer="",
            art_no=code or "",
            art_no_raw=code_raw or "",
            colli=None,
            size_raw="",
            width_cm=None,
            height_cm=None,
            length_cm=None,
            price_eur=price_value,
            barcode="",
            confidence=0.0,
            needs_review=True,
            notes="",
        )
        return row, False, []

    def _split_columns(self, line: str) -> List[str]:
        parts = [part.strip() for part in self.column_split_regex.split(line) if part.strip()]
        if len(parts) <= 1:
            return [line.strip()] if line.strip() else []
        return parts

    def _detect_header_map(self, columns: List[str]) -> Dict[str, int]:
        mapping: Dict[str, int] = {}
        for idx, column in enumerate(columns):
            if label_utils.count_label_hits(self.label_patterns, "code", column):
                mapping["code"] = idx
            if label_utils.count_label_hits(self.label_patterns, "description", column):
                mapping["description"] = idx
            if label_utils.count_label_hits(self.label_patterns, "price", column):
                mapping["price"] = idx
        if len(mapping) >= 2:
            return mapping
        return {}

    def _is_header_like(self, line: str) -> bool:
        if self.price_regex.search(line):
            return False
        label_hits = (
            label_utils.count_label_hits(self.label_patterns, "code", line)
            + label_utils.count_label_hits(self.label_patterns, "description", line)
            + label_utils.count_label_hits(self.label_patterns, "price", line)
        )
        return label_hits >= 2

    def _get_column(self, columns: List[str], field: str) -> str:
        idx = self.header_map.get(field)
        if idx is None or idx >= len(columns):
            return ""
        return columns[idx]

    def _parse_price_column(self, value: str) -> Optional[float]:
        matches = list(self.price_regex.finditer(value or ""))
        if not matches:
            return None
        return text_utils.parse_price(matches[-1].group(1))
