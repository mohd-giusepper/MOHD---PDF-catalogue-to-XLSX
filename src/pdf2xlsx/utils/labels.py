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
        "art_no": [
            "re:\\bart\\.?\\s*no\\.?\\b",
            "art no",
            "art. no",
            "article no",
            "article number",
            "articolo",
            "articolo n",
            "articolo nr",
            "articolo no",
            "part no",
            "part number",
        ],
        "rrp": ["rrp", "list price", "prezzo listino", "msrp", "srp"],
        "colli": ["colli", "collo", "packing", "pack", "package"],
        "designer": ["designer", "designed by", "design"],
        "code": [
            "code",
            "cod",
            "cod.",
            "codice",
            "codice prodotto",
            "codice articolo",
            "item no",
            "item nr",
            "item #",
            "reference",
            "ref",
            "ref.",
            "sku",
            "model",
        ],
        "description": [
            "description",
            "descrizione",
            "descrizione prodotto",
            "descrizione articolo",
            "product name",
            "product description",
            "item description",
            "item name",
            "name",
            "denominazione",
            "nome prodotto",
        ],
        "price": [
            "price",
            "unit price",
            "net price",
            "gross price",
            "prezzo",
            "prezzo netto",
            "prezzo lordo",
            "prezzo unitario",
            "list price",
            "price list",
            "recommended price",
            "retail price",
            "public price",
            "prix",
            "preis",
            "precio",
            "pvp",
            "uvp",
        ],
        "currency": [
            "currency",
            "valuta",
            "eur",
            "dkk",
            "sek",
            "nok",
            "usd",
            "gbp",
            "chf",
            "jpy",
            "cad",
            "aud",
        ],
        "size": [
            "size",
            "dimensions",
            "dimension",
            "dim.",
            "dimensioni",
            "dimensiones",
            "misure",
            "misura",
            "measurements",
        ],
        "material": ["material", "materiale", "materia", "finish", "finitura"],
        "color": ["color", "colour", "colore", "farbe", "couleur"],
        "notes": ["notes", "note", "remarks", "osservazioni", "note tecniche"],
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
