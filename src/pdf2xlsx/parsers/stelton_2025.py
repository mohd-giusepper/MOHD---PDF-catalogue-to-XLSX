import logging
import re
from typing import List, Optional, Tuple

from pdf2xlsx import config
from pdf2xlsx.models import ProductRow
from pdf2xlsx.parsers.base import BaseParser
from pdf2xlsx.utils import text as text_utils


class Stelton2025Parser(BaseParser):
    name = "stelton_2025"
    rrp_pattern = re.compile(r"\*\*\s*RRP", re.IGNORECASE)
    art_no_value_regex = re.compile(
        r"Art\.?\s*no\.?\s*:\s*([A-Z0-9]+(?:-[A-Z0-9]+)*)",
        re.IGNORECASE,
    )

    def parse_block(
        self, raw_text: str, page: int, section: str, source_file: str
    ) -> Tuple[ProductRow, bool, List[str]]:
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]

        art_no, art_no_raw = self.parse_art_no(raw_text)
        colli = self.parse_colli(raw_text)
        size_raw = self.parse_size_raw(raw_text)
        designer = self.parse_designer(raw_text)
        prices, invalid_price_codes = self.parse_prices(raw_text)

        product_name_en, product_name_raw = self.parse_product_name(lines)
        product_name_en, variant = self.extract_variant(product_name_en)

        width_cm, height_cm, length_cm = self.parse_size_dimensions(size_raw)
        size_unparsed = bool(size_raw and not any([width_cm, height_cm, length_cm]))

        row = ProductRow(
            source_file=source_file,
            page=page,
            section=section or "",
            product_name_en=product_name_en or "",
            product_name_raw=product_name_raw or "",
            variant=variant or "",
            designer=designer or "",
            art_no=art_no or "",
            art_no_raw=art_no_raw or "",
            colli=colli,
            size_raw=size_raw or "",
            width_cm=width_cm,
            height_cm=height_cm,
            length_cm=length_cm,
            price_dkk=prices.get("DKK"),
            price_sek=prices.get("SEK"),
            price_nok=prices.get("NOK"),
            price_eur=prices.get("EUR"),
            barcode="",
            confidence=0.0,
            needs_review=True,
            notes="",
        )

        parse_notes = []
        for code in invalid_price_codes:
            parse_notes.append(f"invalid_price_{code.lower()}")

        return row, size_unparsed, parse_notes

    def parse_art_no(self, text: str) -> Tuple[str, str]:
        logger = logging.getLogger(__name__)
        for line in text.splitlines():
            if re.search(r"Art\.?\s*no\.?\s*:", line, re.IGNORECASE):
                logger.info("art_no_line: %s", line.strip())
                match = self.art_no_value_regex.search(line)
                if not match:
                    logger.warning("Art. no. line not parsed: %s", line.strip())
                    return "", ""
                value = match.group(1).strip()
                if not re.search(r"\d", value):
                    logger.warning("Art. no. value missing digits: %s", line.strip())
                    return "", ""
                canonical = self.validate_art_no(value, raw_value=line.strip())
                large_token = ""
                large_match = re.search(r"\b\d{6,}\b", line)
                if large_match:
                    large_token = large_match.group(0)
                logger.info(
                    "art_no_match raw=%s canonical=%s matched_art_no=%s first_large_number_token=%s",
                    value,
                    canonical,
                    value,
                    large_token,
                )
                return canonical, value
        return "", ""

    def parse_colli(self, text: str) -> Optional[int]:
        match = re.search(r"Colli\s*:\s*(\d+)", text, re.IGNORECASE)
        if not match:
            return None
        return int(match.group(1))

    def parse_size_raw(self, text: str) -> str:
        match = re.search(r"^Size\s*:\s*(.*)$", text, re.IGNORECASE | re.MULTILINE)
        if not match:
            return ""
        value = match.group(1).strip()
        if value:
            return value
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            if re.search(r"^Size\s*:\s*$", line, re.IGNORECASE):
                for follow_line in lines[idx + 1 : idx + 3]:
                    if follow_line.strip():
                        return follow_line.strip()
        return ""

    def parse_designer(self, text: str) -> str:
        match = re.search(
            r"^Designer\s*:\s*(.*)$", text, re.IGNORECASE | re.MULTILINE
        )
        if not match:
            return ""
        value = match.group(1).strip()
        if value:
            return value
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            if re.search(r"^Designer\s*:\s*$", line, re.IGNORECASE):
                for follow_line in lines[idx + 1 : idx + 3]:
                    if follow_line.strip():
                        return follow_line.strip()
        return ""

    def parse_prices(self, text: str) -> Tuple[dict, List[str]]:
        prices = {}
        invalid_codes: List[str] = []
        for match in self.price_pattern.finditer(text):
            code = match.group(1).upper()
            price_value = text_utils.parse_price(
                match.group(2),
                min_value=config.PRICE_MIN,
                max_value=config.PRICE_MAX,
            )
            if price_value is None:
                invalid_codes.append(code)
                continue
            prices[code] = price_value
        return prices, invalid_codes

    def parse_product_name(self, lines: List[str]) -> Tuple[str, str]:
        title_lines = []
        for line in lines:
            if self.rrp_pattern.search(line):
                parts = self.rrp_pattern.split(line, maxsplit=1)
                candidate = parts[0].strip()
                if candidate:
                    title_lines.append(candidate)
                continue
            if re.search(r"Colli\s*:", line, re.IGNORECASE):
                break
            if self.is_marker_line(line) or self.is_price_line(line):
                continue
            title_lines.append(line)

        product_name_en = title_lines[0] if title_lines else ""
        product_name_raw = " | ".join(title_lines)
        return product_name_en.strip(), product_name_raw.strip()

    def segment_blocks(self, lines: List[str]) -> List[str]:
        blocks = []
        current: List[str] = []
        for line in lines:
            if self.rrp_pattern.search(line):
                parts = self.rrp_pattern.split(line, maxsplit=1)
                candidate = parts[0].strip()
                if candidate:
                    if current:
                        blocks.append("\n".join(current))
                    current = [line]
                else:
                    if current:
                        current.append(line)
            else:
                if current:
                    current.append(line)
        if current:
            blocks.append("\n".join(current))

        if not blocks:
            return super().segment_blocks(lines)

        split_blocks: List[str] = []
        for block in blocks:
            split_blocks.extend(self._split_by_art_no_positions(block))

        return [block for block in split_blocks if block.strip()]

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

    def extract_variant(self, product_name_en: str) -> Tuple[str, str]:
        if not product_name_en:
            return "", ""

        variant_tokens = {
            "steel",
            "stainless",
            "light grey",
            "dark grey",
            "grey",
            "black",
            "white",
            "clear",
            "transparent",
            "smoked",
            "oak",
            "beech",
            "ash",
            "brass",
            "chrome",
            "polished",
            "matte",
            "satin",
            "glass",
            "porcelain",
            "aluminum",
            "aluminium",
            "plastic",
            "silicone",
            "rubber",
            "felt",
            "leather",
            "walnut",
            "natural",
        }

        name = product_name_en.strip()
        normalized = name.lower().replace("-", " ")

        separator_match = re.match(r"^(.*?)(?:\s*[-,/]\s*)([^-/,]+)$", name)
        if separator_match:
            base = separator_match.group(1).strip()
            tail = separator_match.group(2).strip()
            if self.is_variant_like(tail, variant_tokens):
                return base, tail

        paren_match = re.match(r"^(.*)\(([^)]+)\)\s*$", name)
        if paren_match:
            base = paren_match.group(1).strip()
            tail = paren_match.group(2).strip()
            if self.is_variant_like(tail, variant_tokens):
                return base, tail

        words = normalized.split()
        if len(words) >= 2:
            last_two = " ".join(words[-2:])
            if last_two in variant_tokens:
                return " ".join(name.split()[:-2]).strip(), " ".join(name.split()[-2:]).strip()
        if words:
            last = words[-1]
            if last in variant_tokens:
                return " ".join(name.split()[:-1]).strip(), name.split()[-1].strip()

        return name, ""

    def is_variant_like(self, tail: str, variant_tokens: set) -> bool:
        if not tail or len(tail) > 25:
            return False
        cleaned = tail.lower().replace("-", " ")
        if cleaned in variant_tokens:
            return True
        tokens = cleaned.split()
        if all(token in variant_tokens for token in tokens):
            return True
        if re.search(r"\d", cleaned) and re.search(r"\b(cm|mm|l)\b", cleaned):
            return True
        if re.search(r"\b[0-9]+(?:[.,][0-9]+)?\s*(?:l|cm|mm)\b", cleaned):
            return True
        if re.search(r"\b[o]\s*[0-9]+(?:[.,][0-9]+)?\s*cm\b", cleaned):
            return True
        return False

    def parse_size_dimensions(
        self, size_raw: str
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        if not size_raw:
            return None, None, None

        width = text_utils.extract_dimension(size_raw, "W")
        height = text_utils.extract_dimension(size_raw, "H")
        length = text_utils.extract_dimension(size_raw, "L")
        if length is None:
            length = text_utils.extract_dimension(size_raw, "D")

        if any([width, height, length]):
            return width, height, length

        labeled_match = re.search(
            r"W\s*([0-9.,]+)\s*(?:cm)?\s*[xX]\s*H\s*([0-9.,]+)\s*(?:cm)?\s*[xX]\s*L\s*([0-9.,]+)\s*cm",
            size_raw,
            re.IGNORECASE,
        )
        if labeled_match:
            width = text_utils.parse_number(labeled_match.group(1))
            height = text_utils.parse_number(labeled_match.group(2))
            length = text_utils.parse_number(labeled_match.group(3))
            return width, height, length

        labeled_match = re.search(
            r"L\s*([0-9.,]+)\s*(?:cm)?\s*[xX]\s*W\s*([0-9.,]+)\s*(?:cm)?\s*[xX]\s*H\s*([0-9.,]+)\s*cm",
            size_raw,
            re.IGNORECASE,
        )
        if labeled_match:
            length = text_utils.parse_number(labeled_match.group(1))
            width = text_utils.parse_number(labeled_match.group(2))
            height = text_utils.parse_number(labeled_match.group(3))
            return width, height, length

        diameter_match = re.search(r"(?:O)\s*([0-9.,]+)\s*cm", size_raw)
        if diameter_match:
            diameter = text_utils.parse_number(diameter_match.group(1))
            return diameter, None, None

        return None, None, None
