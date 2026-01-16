import json
import logging
import re
from pathlib import Path
from typing import Dict

from pdf2xlsx import config


LOGGER = logging.getLogger(__name__)

DEFAULT_LABEL_DICTIONARY = {
    "version": "1.0",
    "fields": {
        "art_no": ["re:\\bart\\.?\\s*no\\.?\\b"],
        "rrp": ["rrp", "list price", "prezzo listino"],
        "colli": ["colli", "packing", "pack"],
        "designer": ["designer", "designed by"],
        "code": ["code", "cod", "codice", "item no", "item"],
        "description": ["description", "descrizione", "product name", "name"],
        "price": ["price", "prezzo", "eur", "dkk", "sek", "nok", "€"],
        "size": ["size", "dimension", "dimensioni", "misure"],
    },
    "stopwords": [
        "terms",
        "conditions",
        "indice",
        "index",
        "contents",
        "table of contents",
    ],
}


def load_label_dictionary() -> dict:
    path = Path(config.LABEL_DICTIONARY_PATH)
    return _load_dictionary(path, DEFAULT_LABEL_DICTIONARY)


def load_profile_dictionary(profile_id: str) -> dict:
    if not profile_id:
        return DEFAULT_LABEL_DICTIONARY
    base = load_label_dictionary()
    profile_path = Path(config.PROFILES_DIR) / f"{profile_id}.json"
    profile = _load_dictionary(profile_path, {})
    merged = {
        "version": profile.get("version") or base.get("version"),
        "fields": merge_fields(base.get("fields", {}), profile.get("fields", {})),
        "stopwords": profile.get("stopwords") or base.get("stopwords", []),
    }
    return merged


def merge_fields(base_fields: Dict, extra_fields: Dict) -> Dict:
    merged = {key: list(values) for key, values in (base_fields or {}).items()}
    for field, labels in (extra_fields or {}).items():
        merged.setdefault(field, [])
        for label in labels or []:
            if label not in merged[field]:
                merged[field].append(label)
    return merged


def build_label_patterns(fields: dict) -> dict:
    patterns = {}
    if not fields:
        fields = DEFAULT_LABEL_DICTIONARY.get("fields", {})
    for field, labels in fields.items():
        parts = []
        for label in labels or []:
            if isinstance(label, str) and label.startswith("re:"):
                parts.append(label[3:])
                continue
            if not isinstance(label, str) or not label.strip():
                continue
            cleaned = re.escape(label.strip())
            cleaned = cleaned.replace("\\ ", r"\s+")
            if cleaned[0].isalnum():
                cleaned = r"\b" + cleaned
            if cleaned[-1].isalnum():
                cleaned = cleaned + r"\b"
            parts.append(cleaned)
        if parts:
            patterns[field] = re.compile("|".join(parts), re.IGNORECASE)
    return patterns


def count_label_hits(patterns: dict, field: str, text: str) -> int:
    pattern = patterns.get(field)
    if not pattern:
        return 0
    return len(pattern.findall(text))


def _load_dictionary(path: Path, fallback: dict) -> dict:
    if not path.exists():
        if fallback is DEFAULT_LABEL_DICTIONARY:
            LOGGER.warning("Label dictionary not found: %s", path)
        return fallback
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except json.JSONDecodeError:
        LOGGER.warning("Invalid label dictionary JSON: %s", path)
        return fallback
    if not isinstance(data, dict):
        return fallback
    return data
