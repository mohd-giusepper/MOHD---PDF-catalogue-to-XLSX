import json
import re
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pdf2xlsx import config
from pdf2xlsx.core import page_cache
from pdf2xlsx.utils import labels as label_utils


HEADER_SPLIT_RE = re.compile(r"[^A-Za-z]+")
MIN_TOKEN_LEN = 3
MIN_TOKEN_COUNT = 2


def suggest_profile_for_pdf(
    pdf_path: str,
    ocr: bool = False,
) -> Tuple[dict, dict]:
    label_dict = label_utils.load_label_dictionary()
    stopwords = label_dict.get("stopwords") or config.TRIAGE_STOPWORDS
    cached_pages, _ = page_cache.build_sample_cache(
        pdf_path,
        max_pages=config.TRIAGE_SAMPLE_PAGES_MAX,
        min_text_len=config.TRIAGE_TEXT_LEN_MIN,
        stopwords=stopwords,
        ocr=ocr,
    )
    profile = build_profile_from_pages(cached_pages, label_dict)
    stats = {
        "pages_used": len(cached_pages),
        "tokens_scanned": profile.get("_tokens_scanned", 0),
        "fields_suggested": len(profile.get("fields", {})),
    }
    profile.pop("_tokens_scanned", None)
    return profile, stats


def build_profile_from_pages(cached_pages, label_dict: dict) -> dict:
    token_counter: Counter = Counter()
    for page in cached_pages:
        for line in page.lines:
            if not line:
                continue
            if len(line) > 80:
                continue
            if _is_probable_header(line):
                tokens = [
                    token.lower()
                    for token in HEADER_SPLIT_RE.split(line)
                    if len(token) >= MIN_TOKEN_LEN
                ]
                for token in tokens:
                    token_counter[token] += 1

    existing_terms = _collect_existing_terms(label_dict)
    fields: Dict[str, List[str]] = {}
    for token, count in token_counter.items():
        if count < MIN_TOKEN_COUNT:
            continue
        field = guess_field(token)
        if not field:
            continue
        if token in existing_terms.get(field, set()):
            continue
        fields.setdefault(field, []).append(token)

    for field, tokens in fields.items():
        fields[field] = sorted(set(tokens))

    return {
        "version": "1.0",
        "fields": {field: values for field, values in fields.items() if values},
        "_tokens_scanned": sum(token_counter.values()),
    }


def write_profile(profile_id: str, profile: dict, output_dir: Optional[str] = None) -> str:
    root = Path(output_dir or config.PROFILES_DIR)
    root.mkdir(parents=True, exist_ok=True)
    profile_path = root / f"{profile_id}.json"
    with profile_path.open("w", encoding="utf-8") as handle:
        json.dump(profile, handle, ensure_ascii=True, indent=2)
    return str(profile_path)


def build_profile_id(source_name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", source_name).strip("_")
    if not cleaned:
        cleaned = "profile"
    return f"generated_{cleaned.lower()}"


def _collect_existing_terms(label_dict: dict) -> Dict[str, set]:
    existing: Dict[str, set] = {}
    for field, values in (label_dict.get("fields") or {}).items():
        existing.setdefault(field, set()).update(_normalize_terms(values))
    return existing


def _normalize_terms(values: List[str]) -> List[str]:
    normalized = []
    for value in values:
        if value.startswith("re:"):
            continue
        cleaned = value.strip().lower()
        if cleaned:
            normalized.append(cleaned)
    return normalized


def _is_probable_header(line: str) -> bool:
    if not line:
        return False
    if re.search(r"\b\d{1,7}(?:[.,]\d{2})\b", line):
        return False
    alpha_tokens = re.findall(r"[A-Za-z]{3,}", line)
    if len(alpha_tokens) < 2:
        return False
    if "  " in line or "\t" in line:
        return True
    return line.isupper()


def guess_field(token: str) -> str:
    token = token.lower()
    if token.startswith("art"):
        return "art_no"
    if token.startswith("rrp") or token.startswith("list"):
        return "rrp"
    if token.startswith("coll") or token.startswith("pack"):
        return "colli"
    if token.startswith("desig"):
        return "designer"
    if token.startswith("cod") or token.startswith("item"):
        return "code"
    if token.startswith("desc") or token.startswith("name") or token.startswith("prod"):
        return "description"
    if token.startswith("price") or token.startswith("prezz") or token in {"eur", "dkk", "sek", "nok"}:
        return "price"
    if token.startswith("size") or token.startswith("dim"):
        return "size"
    return ""
