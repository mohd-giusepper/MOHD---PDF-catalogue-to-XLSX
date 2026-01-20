import json
import logging
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pdfplumber

from pdf2xlsx import config
from pdf2xlsx.core import auto_convert, page_cache, triage
from pdf2xlsx.core import suggestions as suggestions_utils
from pdf2xlsx.io import corpus_report
from pdf2xlsx.utils import labels as label_utils


LOGGER = logging.getLogger(__name__)

KNOWN_CURRENCIES = {
    "EUR",
    "DKK",
    "SEK",
    "NOK",
    "USD",
    "GBP",
    "CHF",
    "JPY",
    "CAD",
    "AUD",
}

HEADER_TOKEN_RE = re.compile(r"[A-Za-z]{3,}")

LANG_HINTS = {
    "it": {
        "prezzo",
        "codice",
        "descrizione",
        "dimensioni",
        "misure",
        "colore",
        "pezzi",
        "confezione",
        "quantita",
        "altezza",
        "larghezza",
        "profondita",
    },
    "en": {
        "price",
        "code",
        "description",
        "dimensions",
        "size",
        "width",
        "height",
        "depth",
        "color",
        "qty",
        "pack",
        "pieces",
        "collection",
    },
    "fr": {"prix", "code", "description", "dimensions", "taille"},
    "de": {"preis", "artikel", "beschreibung", "masse", "groesse", "breite", "hoehe", "tiefe"},
}

STOPWORDS = {
    "and",
    "the",
    "with",
    "without",
    "for",
    "from",
    "this",
    "that",
    "these",
    "those",
    "all",
    "new",
    "set",
    "use",
    "usage",
    "guide",
    "index",
    "contents",
    "table",
    "list",
    "price",
    "prices",
    "catalog",
    "catalogue",
    "catalogo",
    "listino",
    "prezzi",
    "prezzo",
    "guid",
    "guida",
    "elenco",
    "informazioni",
    "info",
    "nota",
    "note",
    "notes",
    "indicazioni",
    "funzioni",
    "applicazioni",
    "consigliamo",
    "seguenti",
    "applicazioni",
    "materials",
    "materiali",
    "materiales",
    "materialen",
    "maintenance",
    "mantenimiento",
    "manutenzione",
    "pflege",
    "fabric",
    "pdf",
}

PROFILE_PARSER_MAP = {
    "stelton_marker": "marker_based",
    "table_based": "table_based",
    "code_price_based": "code_price_based",
}

PARSER_TO_STRATEGY = {
    "stelton_2025": "marker_based",
    "table_based": "table_based",
    "code_price_based": "code_price_based",
}


def scan_corpus(
    input_path: str,
    output_dir: str,
) -> Tuple[List[dict], dict, List[dict]]:
    pdf_paths = collect_pdfs(input_path)
    if not pdf_paths:
        raise ValueError("No PDFs found for corpus scan.")

    label_dict = label_utils.load_label_dictionary()
    marker_dict = label_utils.load_profile_dictionary("stelton_marker")
    code_dict = label_utils.load_profile_dictionary("code_price_based")
    marker_patterns = label_utils.build_label_patterns(marker_dict.get("fields", {}))
    code_patterns = label_utils.build_label_patterns(code_dict.get("fields", {}))
    base_patterns = label_utils.build_label_patterns(label_dict.get("fields", {}))
    existing_terms = collect_existing_terms(label_dict)
    stopwords = label_dict.get("stopwords") or config.TRIAGE_STOPWORDS

    rows: List[dict] = []
    missing_counter: Counter = Counter()
    missing_examples: Dict[str, List[dict]] = defaultdict(list)

    output_root = Path(output_dir)
    suggestions_dir = output_root / "profile_suggestions"
    suggestions_dir.mkdir(parents=True, exist_ok=True)

    for pdf_path in pdf_paths:
        page_count = get_page_count(pdf_path)
        cached_pages, page_notes, _ = page_cache.build_signal_cache(
            pdf_path,
            max_pages=config.TRIAGE_TOP_K_MAX,
            min_text_len=config.TRIAGE_TEXT_LEN_MIN,
            stopwords=stopwords,
            ocr=False,
        )

        triage_result = triage.scan_cached_pages(
            pdf_path=pdf_path,
            cached_pages=cached_pages,
            page_notes=page_notes,
            marker_patterns=marker_patterns,
            code_patterns=code_patterns,
        )

        page_numbers = [page.page_number for page in cached_pages]
        combined_text = "\n".join(page.text or "" for page in cached_pages)
        normalized_text = "\n".join(page.normalized_text or "" for page in cached_pages)
        digits_count = sum(1 for char in combined_text if char.isdigit())
        text_len = len(combined_text)
        numeric_density = digits_count / text_len if text_len else 0.0

        price_pattern_hits = sum(
            len(triage.PRICE_RE.findall(page.normalized_text or ""))
            for page in cached_pages
        )

        currency_counts = collect_currency_tokens(combined_text)

        table_columns, numeric_ratio = compute_table_metrics(cached_pages)
        table_likelihood = table_columns + numeric_ratio * 5.0

        size_hits = label_utils.count_label_hits(base_patterns, "size", normalized_text)
        marker_hits = format_marker_hits(
            art_no=triage_result.art_no_count,
            code=triage_result.code_label_count,
            rrp=triage_result.rrp_count,
            size=size_hits,
        )

        recommended_parser = recommend_parser(
            cached_pages=cached_pages,
            triage_result=triage_result,
        )

        risk_flags = build_risk_flags(
            cached_pages=cached_pages,
            page_notes=page_notes,
            text_len_total=triage_result.text_len_total,
        )

        record_missing_labels(
            cached_pages=cached_pages,
            existing_terms=existing_terms,
            missing_counter=missing_counter,
            missing_examples=missing_examples,
            source_path=pdf_path,
        )

        if triage_result.decision in {"FORSE", "NO"} or recommended_parser == "unknown":
            suggestions = build_profile_suggestions(
                pdf_path=pdf_path,
                cached_pages=cached_pages,
                triage_result=triage_result,
                recommended_parser=recommended_parser,
            )
            suggestions_path = suggestions_dir / f"{Path(pdf_path).stem}.json"
            with suggestions_path.open("w", encoding="utf-8") as handle:
                json.dump(suggestions, handle, ensure_ascii=True, indent=2)

        rows.append(
            {
                "path": str(pdf_path),
                "page_count": page_count,
                "pages_sampled": page_numbers,
                "marker_hits": marker_hits,
                "currency_tokens": format_currency_tokens(currency_counts),
                "numeric_density": round(numeric_density, 4),
                "price_pattern_hits": price_pattern_hits,
                "table_likelihood": format_table_likelihood(table_columns, numeric_ratio, table_likelihood),
                "recommended_parser": recommended_parser,
                "risk_flags": ";".join(risk_flags),
                "_table_columns": table_columns,
                "_numeric_ratio": numeric_ratio,
                "_marker_score": triage_result.marker_score,
                "_code_price_score": triage_result.code_price_score,
                "_table_score": triage_result.table_score,
                "_price_hits": price_pattern_hits,
            }
        )

    corpus_report_path = output_root / "corpus_report.xlsx"
    corpus_report.write_corpus_report(rows, str(corpus_report_path))

    groups_payload = build_groups(rows)
    groups_path = output_root / "corpus_groups.json"
    with groups_path.open("w", encoding="utf-8") as handle:
        json.dump(groups_payload, handle, ensure_ascii=True, indent=2)

    missing_payload = build_missing_keys_payload(missing_counter, missing_examples, top_n=20)
    missing_path = output_root / "missing_keys_suggested.json"
    with missing_path.open("w", encoding="utf-8") as handle:
        json.dump(missing_payload, handle, ensure_ascii=True, indent=2)

    summary = {
        "pdf_count": len(pdf_paths),
        "groups_count": len(groups_payload.get("groups", [])),
        "missing_top": missing_payload[:10],
    }
    return rows, groups_payload, missing_payload


def collect_pdfs(input_path: str) -> List[str]:
    base = Path(input_path or config.INPUT_DIR)
    if base.is_file():
        return [str(base)]
    if not base.exists():
        raise ValueError(f"Input path not found: {base}")
    pdfs = sorted(path for path in base.rglob("*.pdf"))
    return [str(path) for path in pdfs]


def get_page_count(pdf_path: str) -> int:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except Exception:
        LOGGER.warning("Failed to read page count for %s", pdf_path)
        return 0


def compute_table_metrics(cached_pages: List[page_cache.CachedPage]) -> Tuple[int, float]:
    table_columns = 0
    numeric_ratios: List[float] = []
    for page in cached_pages:
        columns, ratio = triage.table_metrics_from_words(page.words)
        table_columns = max(table_columns, columns)
        numeric_ratios.append(ratio)
    numeric_ratio = sum(numeric_ratios) / len(numeric_ratios) if numeric_ratios else 0.0
    return table_columns, numeric_ratio


def collect_currency_tokens(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    if not text:
        return counts
    euro_symbol = text.count("\u20ac")
    if euro_symbol:
        counts["EUR_symbol"] = euro_symbol
    for token in re.findall(r"\b[A-Z]{3}\b", text):
        if token in KNOWN_CURRENCIES:
            counts[token] = counts.get(token, 0) + 1
    return counts


def format_currency_tokens(counts: Dict[str, int]) -> str:
    if not counts:
        return ""
    parts = [f"{key}={value}" for key, value in sorted(counts.items())]
    return ";".join(parts)


def format_marker_hits(art_no: int, code: int, rrp: int, size: int) -> str:
    return f"art_no={art_no};code={code};rrp={rrp};size={size}"


def format_table_likelihood(columns: int, ratio: float, score: float) -> str:
    return f"columns={columns};numeric_ratio={ratio:.3f};score={score:.2f}"


def build_risk_flags(
    cached_pages: List[page_cache.CachedPage],
    page_notes: List[str],
    text_len_total: int,
) -> List[str]:
    flags = []
    if not cached_pages:
        flags.append("no_content")
        return flags
    if text_len_total < config.TRIAGE_TEXT_LEN_MIN:
        flags.append("low_text")
    if any(page.needs_ocr for page in cached_pages) and text_len_total < config.TRIAGE_TEXT_LEN_MIN:
        flags.append("scanned_likely")
    if "stopword_pages_skipped" in page_notes and text_len_total < config.TRIAGE_TEXT_LEN_MIN:
        flags.append("intro_only")
    return flags


def collect_existing_terms(label_dict: dict) -> set:
    terms = set()
    for values in (label_dict.get("fields") or {}).values():
        for value in values or []:
            if not isinstance(value, str):
                continue
            if value.startswith("re:"):
                continue
            cleaned = value.strip().lower()
            if cleaned:
                terms.add(cleaned)
    return terms


def record_missing_labels(
    cached_pages: List[page_cache.CachedPage],
    existing_terms: set,
    missing_counter: Counter,
    missing_examples: Dict[str, List[dict]],
    source_path: str,
) -> None:
    for page in cached_pages:
        for line in page.lines:
            clean = (line or "").strip()
            if not clean or len(clean) > 120:
                continue
            if triage.PRICE_RE.search(clean):
                continue
            if not is_label_like(clean):
                continue
            for token in HEADER_TOKEN_RE.findall(clean):
                lowered = token.lower()
                if len(lowered) < 3:
                    continue
                if lowered in STOPWORDS:
                    continue
                if lowered in existing_terms:
                    continue
                missing_counter[lowered] += 1
                if len(missing_examples[lowered]) < 3:
                    missing_examples[lowered].append(
                        {"pdf": str(source_path), "snippet": clean}
                    )


def is_label_like(line: str) -> bool:
    if not line:
        return False
    if len(line) > 90:
        return False
    if line.endswith(":"):
        return True
    if "  " in line or "\t" in line:
        return True
    if line.isupper():
        alpha_tokens = HEADER_TOKEN_RE.findall(line)
        return 2 <= len(alpha_tokens) <= 4
    return False


def estimate_language(token: str) -> str:
    for lang, words in LANG_HINTS.items():
        if token in words:
            return lang
    return "unknown"


def build_missing_keys_payload(
    counter: Counter, examples: Dict[str, List[dict]], top_n: int
) -> List[dict]:
    payload = []
    for token, count in counter.most_common(top_n):
        payload.append(
            {
                "label": token,
                "count": count,
                "lang": estimate_language(token),
                "examples": examples.get(token, [])[:3],
            }
        )
    return payload


def recommend_parser(
    cached_pages: List[page_cache.CachedPage],
    triage_result,
) -> str:
    if not cached_pages:
        return "unknown"
    ordered = auto_convert.order_profiles(triage_result)
    attempt_results = []
    for profile_id in ordered:
        parser_name = auto_convert.PROFILE_PARSER_MAP.get(profile_id, "")
        if not parser_name:
            continue
        eval_result = auto_convert.evaluate_parser_fast(
            cached_pages=cached_pages,
            parser_name=parser_name,
            source_file=triage_result.source_file,
            currency=config.TARGET_CURRENCY,
            currency_only="",
            triage_result=triage_result,
        )
        attempt_results.append(eval_result)
        if eval_result.get("ok") and auto_convert.is_excellent(
            eval_result.get("metrics", {})
        ):
            break
    selected = auto_convert.select_best_run(attempt_results)
    if selected and selected.get("parser"):
        return PARSER_TO_STRATEGY.get(selected.get("parser"), "unknown")
    return PROFILE_PARSER_MAP.get(triage_result.suggested_profile, "unknown")


def build_profile_suggestions(
    pdf_path: str,
    cached_pages: List[page_cache.CachedPage],
    triage_result,
    recommended_parser: str,
) -> dict:
    base = suggestions_utils.build_suggestions(
        pdf_path=pdf_path,
        cached_pages=cached_pages,
        reason=triage_result.reasons or "triage_forse_no",
    )
    return {
        "source_file": Path(pdf_path).name,
        "decision": triage_result.decision,
        "recommended_parser": recommended_parser,
        "motivation": triage_result.reasons,
        "label_candidates": base.get("headers_candidates", []),
        "code_pattern_candidates": base.get("code_candidates", []),
        "price_format_candidates": base.get("price_patterns", {}),
        "column_stats": base.get("column_stats", {}),
    }


def build_groups(rows: Iterable[dict]) -> dict:
    groups: Dict[str, dict] = {}
    for row in rows:
        signature = build_group_signature(row)
        group = groups.setdefault(
            signature,
            {
                "group_id": signature,
                "size": 0,
                "features_sum": {
                    "numeric_density": 0.0,
                    "table_columns": 0.0,
                    "marker_score": 0.0,
                    "table_score": 0.0,
                    "code_price_score": 0.0,
                    "price_hits": 0.0,
                },
                "examples": [],
            },
        )
        group["size"] += 1
        group["features_sum"]["numeric_density"] += float(row.get("numeric_density", 0.0))
        group["features_sum"]["table_columns"] += float(row.get("_table_columns", 0.0))
        group["features_sum"]["marker_score"] += float(row.get("_marker_score", 0.0))
        group["features_sum"]["table_score"] += float(row.get("_table_score", 0.0))
        group["features_sum"]["code_price_score"] += float(row.get("_code_price_score", 0.0))
        group["features_sum"]["price_hits"] += float(row.get("_price_hits", 0.0))
        if len(group["examples"]) < 3:
            group["examples"].append(row.get("path"))

    output_groups = []
    for group in groups.values():
        size = group["size"] or 1
        features_avg = {
            key: round(value / size, 4)
            for key, value in group["features_sum"].items()
        }
        output_groups.append(
            {
                "group_id": group["group_id"],
                "size": group["size"],
                "features_avg": features_avg,
                "examples": group["examples"],
            }
        )

    output_groups.sort(key=lambda item: (-item["size"], item["group_id"]))
    return {"groups": output_groups}


def build_group_signature(row: dict) -> str:
    parser = row.get("recommended_parser", "unknown")
    table_columns = int(row.get("_table_columns", 0) or 0)
    numeric_density = float(row.get("numeric_density", 0.0) or 0.0)
    currency_tokens = row.get("currency_tokens", "") or ""

    if table_columns >= 6:
        columns_bucket = "c6p"
    elif table_columns >= 4:
        columns_bucket = "c4"
    elif table_columns >= 2:
        columns_bucket = "c2"
    elif table_columns == 1:
        columns_bucket = "c1"
    else:
        columns_bucket = "c0"

    if numeric_density >= 0.08:
        numeric_bucket = "nhigh"
    elif numeric_density >= 0.04:
        numeric_bucket = "nmid"
    else:
        numeric_bucket = "nlow"

    currency_count = 0
    for token in currency_tokens.split(";"):
        if not token:
            continue
        currency_count += 1
    if currency_count >= 2:
        currency_bucket = "cmulti"
    elif currency_count == 1:
        currency_bucket = "csingle"
    else:
        currency_bucket = "cnone"

    return f"{parser}_{columns_bucket}_{numeric_bucket}_{currency_bucket}"
