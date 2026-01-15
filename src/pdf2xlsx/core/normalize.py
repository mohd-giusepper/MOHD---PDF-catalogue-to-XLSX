import re
from typing import List


def normalize_text(text: str) -> str:
    text = text.replace("\u00ad", "-")
    text = text.replace("\u00d8", "O").replace("\u00f8", "o")
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def split_lines(text: str) -> List[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def strip_section_line(lines: List[str], section: str) -> List[str]:
    stripped = []
    removed = False
    for line in lines:
        if not removed and line.strip() == section:
            removed = True
            continue
        stripped.append(line)
    return stripped
