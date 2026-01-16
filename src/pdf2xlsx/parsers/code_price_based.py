import re
from typing import List, Optional, Tuple

from pdf2xlsx import config
from pdf2xlsx.models import ProductRow
from pdf2xlsx.parsers.base import BaseParser
from pdf2xlsx.utils import labels as label_utils
from pdf2xlsx.utils import text as text_utils


class CodePriceParser(BaseParser):
    name = "code_price_based"
    price_regex = re.compile(
        r"([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{2})?|[0-9]{1,7}(?:[.,][0-9]{1,2})?)"
    )
    code_token_regex = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-./]*")

    def __init__(self) -> None:
        profile_dict = label_utils.load_profile_dictionary("code_price_based")
        self.label_patterns = label_utils.build_label_patterns(
            profile_dict.get("fields", {})
        )

    def segment_blocks(self, lines: List[str]) -> List[str]:
        blocks = []
        for line in lines:
            if self._is_header_like(line):
                continue
            line_info = text_utils.analyze_line(line)
            if line_info.get("price_like"):
                blocks.append(line)
                continue
            if self._line_has_plausible_code(line):
                blocks.append(line)
        return blocks

    def parse_block(
        self, raw_text: str, page: int, section: str, source_file: str
    ) -> Tuple[ProductRow, bool, List[str]]:
        line = raw_text.strip()
        line_info = text_utils.analyze_line(line)
        parse_notes = []
        if line_info.get("dimension_line"):
            parse_notes.append("dimension_line")
        if not line_info.get("price_like"):
            parse_notes.append("no_price_like")

        price_value, price_raw = self._extract_price(line)
        art_no, art_no_raw = self._extract_code(line, price_raw)
        product_name = self._extract_name(line, art_no_raw, price_raw)

        row = ProductRow(
            source_file=source_file,
            page=page,
            section=section or "",
            product_name_en=product_name or "",
            product_name_raw=line,
            variant="",
            designer="",
            art_no=art_no or "",
            art_no_raw=art_no_raw or "",
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
        return row, False, parse_notes

    def _is_header_like(self, line: str) -> bool:
        if not line or self.price_regex.search(line):
            return False
        label_hits = (
            label_utils.count_label_hits(self.label_patterns, "code", line)
            + label_utils.count_label_hits(self.label_patterns, "description", line)
            + label_utils.count_label_hits(self.label_patterns, "price", line)
            + label_utils.count_label_hits(self.label_patterns, "currency", line)
        )
        return label_hits > 0

    def _extract_price(self, line: str) -> Tuple[Optional[float], str]:
        price_value, raw_value, score = text_utils.pick_price_candidate(line)
        if not raw_value or score <= 0:
            return None, ""
        parsed = text_utils.parse_price(raw_value)
        if parsed is None:
            return None, ""
        return parsed, raw_value

    def _extract_code(self, line: str, price_raw: str) -> Tuple[str, str]:
        tokens = self.code_token_regex.findall(line)
        for token in tokens:
            if price_raw and token == price_raw:
                continue
            if text_utils.is_plausible_code(token, min_len=config.CODE_MIN_LEN):
                return self.validate_art_no(token, raw_value=token), token
        for token in tokens:
            if price_raw and token == price_raw:
                continue
            if text_utils.is_plausible_code(token, min_len=config.CODE_MIN_LEN):
                return self.validate_art_no(token, raw_value=token), token
        return "", ""

    def _extract_name(self, line: str, code_raw: str, price_raw: str) -> str:
        value = line
        if code_raw:
            value = value.replace(code_raw, " ", 1)
        if price_raw:
            value = value.replace(price_raw, " ", 1)
        cleaned = re.sub(r"\s{2,}", " ", value).strip()
        return cleaned

    def _line_has_plausible_code(self, line: str) -> bool:
        tokens = self.code_token_regex.findall(line)
        for token in tokens:
            if text_utils.is_plausible_code(token, min_len=config.CODE_MIN_LEN):
                return True
        return False
