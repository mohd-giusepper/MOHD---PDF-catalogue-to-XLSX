import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import List

from pdf2xlsx.core import page_cache


HEADER_SPLIT_RE = re.compile(r"\s{2,}|\t")
CODE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-./]*")
PRICE_RE = re.compile(r"\b\d{1,7}(?:[.,]\d{2})\b")
EUR_SYMBOL = "\u20ac"


def write_suggestions_if_needed(
    pdf_path: str,
    cached_pages: List[page_cache.CachedPage],
    output_dir: str,
    reason: str,
) -> None:
    suggestions = build_suggestions(pdf_path, cached_pages, reason)
    if not suggestions:
        return
    output_root = Path(output_dir) / "profile_suggestions"
    output_root.mkdir(parents=True, exist_ok=True)
    stem = Path(pdf_path).stem
    output_path = output_root / f"{stem}.json"
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(suggestions, handle, ensure_ascii=True, indent=2)


def build_suggestions(
    pdf_path: str,
    cached_pages: List[page_cache.CachedPage],
    reason: str,
) -> dict:
    if not cached_pages:
        return {}

    header_lines = collect_header_candidates(cached_pages, top_n=15)
    code_candidates = collect_code_candidates(cached_pages, top_n=8)
    price_patterns = collect_price_patterns(cached_pages)
    column_stats = collect_column_stats(cached_pages)

    return {
        "source_file": Path(pdf_path).name,
        "reason": reason,
        "headers_candidates": header_lines,
        "code_candidates": code_candidates,
        "price_patterns": price_patterns,
        "column_stats": column_stats,
    }


def collect_header_candidates(cached_pages: List[page_cache.CachedPage], top_n: int) -> List[dict]:
    counter: Counter = Counter()
    for page in cached_pages:
        for line in page.lines:
            if not line:
                continue
            if PRICE_RE.search(line):
                continue
            if not _is_header_like(line):
                continue
            counter[line.strip()] += 1

    return [
        {"line": line, "count": count}
        for line, count in counter.most_common(top_n)
    ]


def collect_code_candidates(cached_pages: List[page_cache.CachedPage], top_n: int) -> List[dict]:
    pattern_counts: Counter = Counter()
    examples: defaultdict[str, List[str]] = defaultdict(list)
    for page in cached_pages:
        for line in page.lines:
            tokens = CODE_TOKEN_RE.findall(line or "")
            for token in tokens:
                if not re.search(r"\d", token):
                    continue
                bucket = classify_code_pattern(token)
                pattern_counts[bucket] += 1
                if len(examples[bucket]) < 5:
                    examples[bucket].append(token)

    results = []
    for bucket, count in pattern_counts.most_common(top_n):
        results.append({"pattern": bucket, "count": count, "examples": examples[bucket]})
    return results


def collect_price_patterns(cached_pages: List[page_cache.CachedPage]) -> dict:
    separators = Counter()
    currency_tokens = Counter()
    for page in cached_pages:
        text = page.text or ""
        if EUR_SYMBOL in text:
            currency_tokens["EUR_symbol"] += text.count(EUR_SYMBOL)
        for token in re.findall(r"\b[A-Z]{3}\b", text):
            if token in {"EUR", "DKK", "SEK", "NOK"}:
                currency_tokens[token] += 1
        for match in PRICE_RE.findall(text):
            if "," in match and "." in match:
                separators["comma_dot"] += 1
            elif "," in match:
                separators["comma"] += 1
            elif "." in match:
                separators["dot"] += 1

    return {
        "currency_tokens": dict(currency_tokens),
        "decimal_separators": dict(separators),
        "price_regex": PRICE_RE.pattern,
    }


def collect_column_stats(cached_pages: List[page_cache.CachedPage]) -> dict:
    column_counts = Counter()
    stability = []
    for page in cached_pages:
        if not page.words:
            continue
        bins = Counter()
        for word in page.words:
            x0 = word.get("x0")
            if x0 is None:
                continue
            bins[int(x0 // 20)] += 1
        strong_bins = [bin_id for bin_id, count in bins.items() if count >= 8]
        column_counts[len(strong_bins)] += 1
        if strong_bins:
            stability.append(len(strong_bins))

    return {
        "columns_distribution": dict(column_counts),
        "avg_columns": sum(stability) / len(stability) if stability else 0.0,
    }


def classify_code_pattern(token: str) -> str:
    if re.match(r"^\d+$", token):
        return "digits_only"
    if re.match(r"^[A-Za-z]+\\d+$", token):
        return "alpha_digits"
    if re.match(r"^\\d+[A-Za-z]+$", token):
        return "digits_alpha"
    if re.match(r"^[A-Za-z0-9]+[-./][A-Za-z0-9]+", token):
        return "alnum_with_separators"
    return "mixed"


def _is_header_like(line: str) -> bool:
    if not line:
        return False
    if len(line) > 120:
        return False
    parts = [part.strip() for part in HEADER_SPLIT_RE.split(line) if part.strip()]
    if len(parts) >= 2:
        return True
    return line.isupper()
