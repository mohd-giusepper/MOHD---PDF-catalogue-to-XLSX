import logging
import re
from abc import ABC, abstractmethod
from typing import List

from pdf2xlsx.core import segment
from pdf2xlsx.utils import text as text_utils


class BaseParser(ABC):
    name = "base"
    currency_codes = ("DKK", "SEK", "NOK", "EUR")
    art_no_regex = re.compile(r"Art\.?\s*no\.?\s*:", re.IGNORECASE)
    art_no_value_pattern = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-./]*$")
    price_pattern = re.compile(r"\b(DKK|SEK|NOK|EUR)\s+([0-9]+(?:[.,][0-9]{1,2})?)")
    marker_patterns = (
        r"Colli\s*:",
        r"Size\s*:",
        r"Designer\s*:",
        r"Art\.?\s*no\.?\s*:",
        r"\bRRP\b",
    )

    def detect_section(self, lines: List[str]) -> str:
        top_lines = [line.strip() for line in lines[:8] if line.strip()]
        for line in top_lines:
            if self.is_section_line(line):
                return line
        return ""

    def is_section_line(self, line: str) -> bool:
        if not line:
            return False
        lowered = line.lower()
        if any(
            keyword in lowered
            for keyword in ["colli", "size", "designer", "art. no", "rrp"]
        ):
            return False
        if any(term in lowered for term in ["pricelist", "catalog", "stelton"]):
            return False
        if re.search(r"\d", line):
            return False
        if len(line) > 40 or len(line) < 3:
            return False
        if self.is_price_line(line):
            return False
        return True

    def is_marker_line(self, line: str) -> bool:
        return any(re.search(pattern, line, re.IGNORECASE) for pattern in self.marker_patterns)

    def is_price_line(self, line: str) -> bool:
        return bool(self.price_pattern.search(line))

    def contains_art_no(self, text: str) -> bool:
        return bool(self.art_no_regex.search(text))

    def validate_art_no(self, value: str, raw_value: str = "") -> str:
        if not value:
            return ""
        value = text_utils.canonicalize_art_no(value.strip())
        if self.art_no_value_pattern.match(value):
            return value
        raw = raw_value or value
        logging.getLogger(__name__).warning(
            "Discarding art_no value: '%s' (raw: '%s') invalid format",
            value,
            raw,
        )
        return ""

    def split_merged_block(self, text: str):
        matches = list(self.art_no_regex.finditer(text))
        if len(matches) <= 1:
            return [text], ""

        split_blocks = self._split_by_art_no_positions(text)
        if len(split_blocks) > 1:
            return split_blocks, "merged_block_split_by_art_no"
        return [text], "merged_block_unsplit"

    def _split_by_art_no_positions(self, text: str) -> List[str]:
        matches = list(self.art_no_regex.finditer(text))
        if len(matches) <= 1:
            return [text]

        starts = [match.start() for match in matches]
        prefix = text[: starts[0]].strip()
        segments = []
        for idx, start in enumerate(starts):
            end = starts[idx + 1] if idx + 1 < len(starts) else len(text)
            segment = text[start:end].strip()
            if idx == 0 and prefix:
                segment = f"{prefix}\n{segment}"
            segments.append(segment)
        return segments

    def is_title_candidate(self, line: str) -> bool:
        if not line.strip():
            return False
        if self.is_marker_line(line):
            return False
        if self.is_price_line(line):
            return False
        return True

    def lookahead_has_colli(self, lines: List[str], idx: int, window: int = 4) -> bool:
        for look_idx in range(idx, min(len(lines), idx + window + 1)):
            if "Colli" in lines[look_idx]:
                return True
        return False

    def is_block_boundary(self, lines: List[str], idx: int, have_art: bool) -> bool:
        return have_art and self.is_title_candidate(lines[idx]) and self.lookahead_has_colli(lines, idx)

    def is_block_valid(self, text: str) -> bool:
        if self.contains_art_no(text) and "RRP" in text:
            return True
        return "RRP" in text and bool(self.price_pattern.search(text))

    def segment_blocks(self, lines: List[str]) -> List[str]:
        return segment.split_blocks(
            lines=lines,
            is_block_boundary=self.is_block_boundary,
            contains_art_no=self.contains_art_no,
            is_block_valid=self.is_block_valid,
        )

    @abstractmethod
    def parse_block(
        self, raw_text: str, page: int, section: str, source_file: str
    ):
        raise NotImplementedError
