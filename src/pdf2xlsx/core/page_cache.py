import hashlib
import json
import logging
import math
import os
import random
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple

import pdfplumber

from pdf2xlsx import config
from pdf2xlsx.core import extract, normalize
from pdf2xlsx.utils import text as text_utils


_NUMBER_RE = re.compile(r"\d")
_MULTI_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")
_CURRENCY_TOKEN_RE = re.compile(r"\b[A-Z]{3}\b")
_KNOWN_CURRENCIES = {"EUR", "DKK", "SEK", "NOK", "USD", "GBP", "CHF", "JPY", "CAD", "AUD"}
_CODE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-./]*")
LOGGER = logging.getLogger(__name__)


def clear_cache_dir() -> int:
    cache_dir = Path(config.CACHE_DIR)
    if not cache_dir.exists():
        return 0
    removed = 0
    for path in cache_dir.glob("*.json"):
        try:
            path.unlink()
            removed += 1
        except OSError:
            continue
    return removed


def _cache_key(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool,
    sample_multiplier: float,
    scan_mode: str,
) -> str:
    try:
        stat = os.stat(pdf_path)
        mtime = stat.st_mtime
        size = stat.st_size
    except OSError:
        mtime = 0
        size = 0
    stopwords_hash = hashlib.sha1(
        ",".join(sorted(word.lower() for word in stopwords)).encode("utf-8")
    ).hexdigest()
    raw = (
        f"{pdf_path}|{mtime}|{size}|{ocr}|{max_pages}|{min_text_len}|"
        f"{sample_multiplier}|{scan_mode}|{stopwords_hash}|{config.CACHE_VERSION}"
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _cache_path(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool,
    sample_multiplier: float,
    scan_mode: str,
) -> Path:
    cache_dir = Path(config.CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = _cache_key(
        pdf_path,
        max_pages,
        min_text_len,
        stopwords,
        ocr,
        sample_multiplier,
        scan_mode,
    )
    return cache_dir / f"{key}.json"


def _deserialize_cached_pages(items: List[dict]) -> List["CachedPage"]:
    pages: List[CachedPage] = []
    for item in items:
        try:
            pages.append(
                CachedPage(
                    page_number=item.get("page_number"),
                    text=item.get("text") or "",
                    normalized_text=item.get("normalized_text") or "",
                    lines=item.get("lines") or [],
                    words=item.get("words") or [],
                    text_len=item.get("text_len") or 0,
                    images_count=item.get("images_count") or 0,
                    needs_ocr=bool(item.get("needs_ocr")),
                    ocr_used=bool(item.get("ocr_used")),
                    table_hint=bool(item.get("table_hint")),
                    numeric_density=float(item.get("numeric_density") or 0.0),
                    currency_tokens=item.get("currency_tokens") or {},
                    table_likelihood=float(item.get("table_likelihood") or 0.0),
                    signal_score=float(item.get("signal_score") or 0.0),
                    mixed_code_count=int(item.get("mixed_code_count") or 0),
                    price_like_count=int(item.get("price_like_count") or 0),
                    cooccurrence_count=int(item.get("cooccurrence_count") or 0),
                )
            )
        except Exception:
            continue
    return pages


def _load_cached_signal_pages(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool,
    sample_multiplier: float,
    scan_mode: str,
) -> Tuple[List["CachedPage"], List[str], dict]:
    if not config.CACHE_ENABLED:
        return [], [], {}
    cache_path = _cache_path(
        pdf_path,
        max_pages,
        min_text_len,
        stopwords,
        ocr,
        sample_multiplier,
        scan_mode,
    )
    if not cache_path.exists():
        return [], [], {}
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return [], [], {}
    if payload.get("version") != config.CACHE_VERSION:
        return [], [], {}
    pages = _deserialize_cached_pages(payload.get("pages") or [])
    if not pages:
        return [], [], {}
    notes = ["cache_hit"]
    extra_notes = payload.get("notes") or []
    if isinstance(extra_notes, list):
        notes.extend(str(item) for item in extra_notes if item)
    meta = payload.get("meta") or {}
    return pages, notes, meta


def _save_cached_signal_pages(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool,
    sample_multiplier: float,
    scan_mode: str,
    pages: List["CachedPage"],
    notes: List[str],
    meta: dict,
) -> None:
    if not config.CACHE_ENABLED:
        return
    cache_path = _cache_path(
        pdf_path,
        max_pages,
        min_text_len,
        stopwords,
        ocr,
        sample_multiplier,
        scan_mode,
    )
    payload = {
        "version": config.CACHE_VERSION,
        "pages": [asdict(page) for page in pages],
        "notes": notes,
        "meta": meta,
    }
    try:
        cache_path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        return


@dataclass
class CachedPage:
    page_number: int
    text: str
    normalized_text: str
    lines: List[str]
    words: List[dict]
    text_len: int
    images_count: int
    needs_ocr: bool
    ocr_used: bool
    table_hint: bool
    numeric_density: float = 0.0
    currency_tokens: Dict[str, int] = field(default_factory=dict)
    table_likelihood: float = 0.0
    signal_score: float = 0.0
    mixed_code_count: int = 0
    price_like_count: int = 0
    cooccurrence_count: int = 0


def compute_sample_count(num_pages: int, multiplier: float = 1.0) -> int:
    if num_pages <= 0:
        return 0
    if num_pages <= 30:
        ratio = 0.30
        min_count = 8
        max_count = 15
    elif num_pages <= 100:
        ratio = 0.15
        min_count = 12
        max_count = 25
    elif num_pages <= 300:
        ratio = 0.10
        min_count = 20
        max_count = 40
    elif num_pages <= 800:
        ratio = 0.04
        min_count = 30
        max_count = 80
    else:
        ratio = 0.01
        min_count = 40
        max_count = 120
    base = int(math.ceil(num_pages * ratio))
    base = max(min_count, min(max_count, base))
    if multiplier and multiplier > 1.0:
        base = int(math.ceil(base * multiplier))
    base = min(base, config.CACHE_SAMPLE_COUNT_CAP, num_pages)
    return max(1, base)


def compute_top_k_count(sample_count: int) -> int:
    if sample_count <= 0:
        return 0
    target = max(1, sample_count // 2)
    target = max(config.TRIAGE_TOP_K_MIN, min(config.TRIAGE_TOP_K_MAX, target))
    return min(sample_count, target)


def _seed_for_sample(pdf_path: str, sample_multiplier: float, scan_mode: str) -> int:
    raw = f"{pdf_path}|{sample_multiplier}|{scan_mode}"
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]
    return int(digest, 16)


def _select_sweep_pages(num_pages: int) -> Tuple[List[int], int]:
    if num_pages <= 0:
        return [], 0
    step = max(config.CACHE_SWEEP_STEP_MIN, int(math.ceil(num_pages / config.CACHE_SWEEP_TARGET_PAGES)))
    pages = list(range(1, num_pages + 1, step))
    return pages, step


def select_sample_pages(
    num_pages: int,
    sample_count: int,
    seed: int,
) -> List[int]:
    if num_pages <= 0 or sample_count <= 0:
        return []
    if sample_count >= num_pages:
        return list(range(1, num_pages + 1))
    rng = random.Random(seed)
    step = num_pages / sample_count
    start = rng.uniform(0, step)
    raw_pages: List[int] = []
    for idx in range(sample_count):
        base = start + idx * step
        jitter = rng.uniform(-0.25 * step, 0.25 * step)
        candidate = int(round(base + jitter))
        candidate = max(0, min(num_pages - 1, candidate))
        raw_pages.append(candidate + 1)
    pages = sorted(set(raw_pages))
    if len(pages) < sample_count:
        seen = set(pages)
        for page in range(1, num_pages + 1):
            if page in seen:
                continue
            pages.append(page)
            if len(pages) >= sample_count:
                break
    return sorted(pages)


def _count_legal_hits(lower_text: str) -> int:
    if not lower_text:
        return 0
    hits = 0
    for marker in config.CACHE_LEGAL_MARKERS:
        if marker and marker in lower_text:
            hits += 1
    return hits


def _line_signal_counts(lines: List[str]) -> Tuple[int, int, int]:
    price_like_count = 0
    mixed_code_count = 0
    cooccurrence_count = 0
    for line in lines:
        if not line:
            continue
        line_info = text_utils.analyze_line(line)
        price_like = bool(line_info.get("price_like"))
        if price_like:
            price_like_count += 1
        code_count = 0
        for token in _CODE_TOKEN_RE.findall(line):
            if text_utils.is_plausible_code(token, min_len=config.CODE_MIN_LEN):
                code_count += 1
        if code_count:
            mixed_code_count += code_count
        if price_like and code_count:
            cooccurrence_count += 1
    return price_like_count, mixed_code_count, cooccurrence_count


def _compute_signal_score(
    numeric_density: float,
    currency_tokens: Dict[str, int],
    mixed_code_count: int,
    price_like_count: int,
    cooccurrence_count: int,
    table_hint: bool,
    has_stopword: bool,
    legal_hits: int,
) -> float:
    score = (
        cooccurrence_count * 4.0
        + price_like_count * 2.0
        + mixed_code_count * 1.0
        + sum(currency_tokens.values()) * 1.5
        + numeric_density * 40.0
    )
    if table_hint:
        score += 5.0
    if legal_hits:
        score *= config.CACHE_LEGAL_PENALTY
    if has_stopword:
        score *= config.CACHE_STOPWORD_PENALTY
    return score


def _compute_page_signals(
    text: str,
    normalized: str,
    lines: List[str],
    stopwords_lower: List[str],
) -> dict:
    lower_text = (text or "").lower()
    has_stopword = any(word in lower_text for word in stopwords_lower)
    legal_hits = _count_legal_hits(lower_text)
    table_hint = _table_hint(lines)
    numeric_density = _numeric_density(normalized)
    currency_tokens = _collect_currency_tokens(text)
    price_like_count, mixed_code_count, cooccurrence_count = _line_signal_counts(lines)
    signal_score = _compute_signal_score(
        numeric_density=numeric_density,
        currency_tokens=currency_tokens,
        mixed_code_count=mixed_code_count,
        price_like_count=price_like_count,
        cooccurrence_count=cooccurrence_count,
        table_hint=table_hint,
        has_stopword=has_stopword,
        legal_hits=legal_hits,
    )
    return {
        "has_stopword": has_stopword,
        "legal_hits": legal_hits,
        "table_hint": table_hint,
        "numeric_density": numeric_density,
        "currency_tokens": currency_tokens,
        "price_like_count": price_like_count,
        "mixed_code_count": mixed_code_count,
        "cooccurrence_count": cooccurrence_count,
        "signal_score": signal_score,
    }


def build_sample_cache(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool = False,
) -> Tuple[List[CachedPage], List[str]]:
    stopwords_lower = [word.lower() for word in stopwords]
    selected: List[CachedPage] = []
    candidates: List[Tuple[CachedPage, bool]] = []
    notes: List[str] = []
    stopword_hits = 0
    ocr_candidates = 0

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for idx in range(total_pages):
            page_number = idx + 1
            page = pdf.pages[idx]
            text = page.extract_text() or ""
            text_len = len(text.strip())
            lower_text = text.lower()
            has_stopword = any(word in lower_text for word in stopwords_lower)
            images_count = len(getattr(page, "images", []))
            needs_ocr = extract.page_needs_ocr(text_len, images_count)
            ocr_used = False

            if ocr and needs_ocr:
                ocr_text = extract.ocr_page(pdf_path, page_number)
                if ocr_text:
                    text = ocr_text
                    text_len = len(text.strip())
                    ocr_used = True

            normalized = normalize.normalize_text(text)
            lines = normalize.split_lines(normalized)
            table_hint = _table_hint(lines)
            words = []
            if not ocr_used and table_hint:
                words = page.extract_words() or []

            cached_page = CachedPage(
                page_number=page_number,
                text=text,
                normalized_text=normalized,
                lines=lines,
                words=words,
                text_len=text_len,
                images_count=images_count,
                needs_ocr=needs_ocr,
                ocr_used=ocr_used,
                table_hint=table_hint,
            )
            candidates.append((cached_page, has_stopword))

            if has_stopword:
                stopword_hits += 1

            if text_len >= min_text_len and not has_stopword:
                selected.append(cached_page)
            elif ocr and needs_ocr and not has_stopword:
                selected.append(cached_page)
                ocr_candidates += 1

            if len(selected) >= max_pages:
                break

    if not selected and candidates:
        candidates_sorted = sorted(candidates, key=lambda item: item[0].text_len, reverse=True)
        selected = [item[0] for item in candidates_sorted[:max_pages]]
        notes.append("fallback_pages_used")

    if stopword_hits:
        notes.append("stopword_pages_skipped")
    if ocr_candidates:
        notes.append("ocr_candidate_pages")

    return selected, notes


def build_signal_cache(
    pdf_path: str,
    max_pages: int,
    min_text_len: int,
    stopwords: List[str],
    ocr: bool = False,
    sample_multiplier: float = 1.0,
    scan_mode: str = "initial",
    force_rescan: bool = False,
    enable_ocr: bool = False,
) -> Tuple[List[CachedPage], List[str], dict]:
    cached_pages, cached_notes, cached_meta = _load_cached_signal_pages(
        pdf_path=pdf_path,
        max_pages=max_pages,
        min_text_len=min_text_len,
        stopwords=stopwords,
        ocr=ocr,
        sample_multiplier=sample_multiplier,
        scan_mode=scan_mode,
    )
    if cached_pages and not force_rescan:
        return cached_pages, cached_notes, cached_meta

    stopwords_lower = [word.lower() for word in stopwords]
    candidates: List[CachedPage] = []
    stopword_flags: Dict[int, bool] = {}
    notes: List[str] = []
    scanned_pages: List[int] = []
    strong_hits = 0
    sweep_hits: List[int] = []
    ocr_candidates: List[CachedPage] = []
    total_pages = 0

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        seed = _seed_for_sample(pdf_path, sample_multiplier, scan_mode)
        if scan_mode == "sweep":
            sampled_pages, sweep_step = _select_sweep_pages(total_pages)
            notes.append(f"sweep_step={sweep_step}")
        else:
            sample_count = compute_sample_count(total_pages, sample_multiplier)
            sampled_pages = select_sample_pages(total_pages, sample_count, seed)
        sample_count = len(sampled_pages)
        LOGGER.info(
            "SAMPLE_COUNT num_pages=%s sample_count=%s mode=%s",
            total_pages,
            sample_count,
            scan_mode,
        )

        for page_number in sampled_pages:
            page = pdf.pages[page_number - 1]
            text = page.extract_text() or ""
            text_len = len(text.strip())
            images_count = len(getattr(page, "images", []))
            needs_ocr = extract.page_needs_ocr(text_len, images_count)
            normalized = normalize.normalize_text(text)
            lines = normalize.split_lines(normalized)
            signals = _compute_page_signals(text, normalized, lines, stopwords_lower)
            cached_page = CachedPage(
                page_number=page_number,
                text=text,
                normalized_text=normalized,
                lines=lines,
                words=[],
                text_len=text_len,
                images_count=images_count,
                needs_ocr=needs_ocr,
                ocr_used=False,
                table_hint=signals["table_hint"],
                numeric_density=round(signals["numeric_density"], 4),
                currency_tokens=signals["currency_tokens"],
                table_likelihood=0.0,
                signal_score=round(signals["signal_score"], 4),
                mixed_code_count=signals["mixed_code_count"],
                price_like_count=signals["price_like_count"],
                cooccurrence_count=signals["cooccurrence_count"],
            )
            candidates.append(cached_page)
            stopword_flags[page_number] = signals["has_stopword"]
            scanned_pages.append(page_number)
            if (
                not stopword_flags[page_number]
                and cached_page.signal_score >= config.CACHE_EARLY_STOP_SCORE
            ):
                strong_hits += 1
            if (
                scan_mode != "sweep"
                and len(scanned_pages) >= config.CACHE_EARLY_STOP_MIN_PAGES
                and strong_hits >= config.CACHE_EARLY_STOP_STRONG_PAGES
            ):
                notes.append("early_stop")
                break
            if (
                scan_mode == "sweep"
                and cached_page.signal_score >= config.CACHE_SWEEP_HIT_SCORE
            ):
                notes.append("sweep_hit")
                sweep_hits.append(page_number)
                break
            if enable_ocr and ocr and needs_ocr and images_count > 0 and text_len < min_text_len:
                ocr_candidates.append(cached_page)

        if scan_mode == "sweep" and sweep_hits:
            expand_pages = []
            for hit in sweep_hits[:1]:
                for page_number in range(
                    hit - config.CACHE_SWEEP_EXPAND_PAGES,
                    hit + config.CACHE_SWEEP_EXPAND_PAGES + 1,
                ):
                    if page_number < 1 or page_number > total_pages:
                        continue
                    if page_number in scanned_pages:
                        continue
                    expand_pages.append(page_number)
            expand_pages = sorted(set(expand_pages))
            if expand_pages:
                notes.append(f"sweep_expand={len(expand_pages)}")
            for page_number in expand_pages:
                page = pdf.pages[page_number - 1]
                text = page.extract_text() or ""
                text_len = len(text.strip())
                images_count = len(getattr(page, "images", []))
                needs_ocr = extract.page_needs_ocr(text_len, images_count)
                normalized = normalize.normalize_text(text)
                lines = normalize.split_lines(normalized)
                signals = _compute_page_signals(text, normalized, lines, stopwords_lower)
                cached_page = CachedPage(
                    page_number=page_number,
                    text=text,
                    normalized_text=normalized,
                    lines=lines,
                    words=[],
                    text_len=text_len,
                    images_count=images_count,
                    needs_ocr=needs_ocr,
                    ocr_used=False,
                    table_hint=signals["table_hint"],
                    numeric_density=round(signals["numeric_density"], 4),
                    currency_tokens=signals["currency_tokens"],
                    table_likelihood=0.0,
                    signal_score=round(signals["signal_score"], 4),
                    mixed_code_count=signals["mixed_code_count"],
                    price_like_count=signals["price_like_count"],
                    cooccurrence_count=signals["cooccurrence_count"],
                )
                candidates.append(cached_page)
                stopword_flags[page_number] = signals["has_stopword"]
                scanned_pages.append(page_number)
                if enable_ocr and ocr and needs_ocr and images_count > 0 and text_len < min_text_len:
                    ocr_candidates.append(cached_page)
        if scan_mode == "sweep" and not sweep_hits:
            notes.append("sweep_no_hit")

    if not candidates:
        return [], [], {}

    scanned_pages = sorted(set(scanned_pages))
    sample_count = len(scanned_pages)
    stopword_hits = sum(1 for value in stopword_flags.values() if value)
    if stopword_hits:
        notes.append("stopword_pages_skipped")

    if enable_ocr and ocr and ocr_candidates:
        ocr_budget = min(
            config.CACHE_OCR_MAX_PAGES,
            max(1, sample_count // 2),
        )
        ocr_candidates.sort(
            key=lambda page: (page.text_len, -page.images_count)
        )
        for page in ocr_candidates[:ocr_budget]:
            ocr_text = extract.ocr_page(pdf_path, page.page_number)
            if not ocr_text:
                continue
            page.text = ocr_text
            page.text_len = len(ocr_text.strip())
            page.ocr_used = True
            normalized = normalize.normalize_text(ocr_text)
            lines = normalize.split_lines(normalized)
            signals = _compute_page_signals(ocr_text, normalized, lines, stopwords_lower)
            page.normalized_text = normalized
            page.lines = lines
            page.table_hint = signals["table_hint"]
            page.numeric_density = round(signals["numeric_density"], 4)
            page.currency_tokens = signals["currency_tokens"]
            page.signal_score = round(signals["signal_score"], 4)
            page.mixed_code_count = signals["mixed_code_count"]
            page.price_like_count = signals["price_like_count"]
            page.cooccurrence_count = signals["cooccurrence_count"]
            stopword_flags[page.page_number] = signals["has_stopword"]

    if not candidates:
        return [], [], {}

    top_k_count = min(max_pages, compute_top_k_count(sample_count))
    words_top_m = max(config.CACHE_WORDS_TOP_M, top_k_count)
    candidates.sort(key=lambda item: item.signal_score, reverse=True)
    with pdfplumber.open(pdf_path) as pdf:
        for page in candidates[:words_top_m]:
            if not page.table_hint or page.ocr_used:
                continue
            pdf_page = pdf.pages[page.page_number - 1]
            words = pdf_page.extract_words() or []
            columns, ratio = _table_metrics_from_words(words)
            page.words = words
            page.table_likelihood = round(columns + ratio * 5.0, 3)
            page.signal_score = round(
                page.signal_score + page.table_likelihood * config.CACHE_TABLE_LIKELIHOOD_WEIGHT,
                4,
            )

    eligible = [
        page
        for page in candidates
        if page.text_len >= min_text_len and not stopword_flags.get(page.page_number, False)
    ]
    if eligible:
        eligible.sort(key=lambda item: item.signal_score, reverse=True)
        selected = eligible[:top_k_count]
    else:
        candidates.sort(key=lambda item: item.signal_score, reverse=True)
        selected = candidates[:top_k_count]
        notes.append("fallback_pages_used")

    top_pages = [page.page_number for page in selected]
    top_scores = [page.signal_score for page in selected]
    LOGGER.info("TOP_K pages=%s scores=%s", top_pages, top_scores)

    meta = {
        "num_pages": total_pages,
        "sample_count": sample_count,
        "pages_sampled": scanned_pages,
        "top_k_pages": top_pages,
        "top_k_scores": top_scores,
        "scan_mode": scan_mode,
        "sample_multiplier": sample_multiplier,
    }

    _save_cached_signal_pages(
        pdf_path=pdf_path,
        max_pages=max_pages,
        min_text_len=min_text_len,
        stopwords=stopwords,
        ocr=ocr,
        sample_multiplier=sample_multiplier,
        scan_mode=scan_mode,
        pages=selected,
        notes=notes,
        meta=meta,
    )

    return selected, notes, meta


def _table_hint(lines: List[str]) -> bool:
    if not lines:
        return False

    numeric_lines = 0
    spaced_lines = 0
    for line in lines:
        if not line:
            continue
        if _MULTI_NUMBER_RE.findall(line) and len(_MULTI_NUMBER_RE.findall(line)) >= 2:
            numeric_lines += 1
        if "  " in line:
            spaced_lines += 1

    total = len(lines)
    numeric_ratio = numeric_lines / total if total else 0.0
    spaced_ratio = spaced_lines / total if total else 0.0
    if numeric_ratio >= config.TABLE_WORDS_HINT_MIN_RATIO:
        return True
    if spaced_ratio >= config.TABLE_WORDS_HINT_MIN_SPACE_RATIO:
        return True
    return False


def _table_metrics_from_words(words: List[dict]) -> Tuple[int, float]:
    if not words:
        return 0, 0.0

    bin_size = config.TRIAGE_TABLE_X_BIN_SIZE
    y_bin_size = config.TRIAGE_TABLE_Y_BIN_SIZE
    bin_counts: Counter[int] = Counter()
    for word in words:
        x0 = word.get("x0")
        if x0 is None:
            continue
        bin_id = int(x0 // bin_size)
        bin_counts[bin_id] += 1

    strong_bins = {
        bin_id
        for bin_id, count in bin_counts.items()
        if count >= config.TRIAGE_TABLE_MIN_BIN_HITS
    }
    column_count = len(strong_bins)
    if column_count == 0:
        return 0, 0.0

    line_bins: defaultdict[int, List[dict]] = defaultdict(list)
    for word in words:
        top = word.get("top")
        if top is None:
            continue
        line_id = int(top // y_bin_size)
        line_bins[line_id].append(word)

    numeric_line_count = 0
    for line_words in line_bins.values():
        numeric_bins = set()
        for word in line_words:
            text = (word.get("text") or "").strip()
            if not text or not _NUMBER_RE.search(text):
                continue
            x0 = word.get("x0")
            if x0 is None:
                continue
            bin_id = int(x0 // bin_size)
            if bin_id in strong_bins:
                numeric_bins.add(bin_id)
        if len(numeric_bins) >= 2:
            numeric_line_count += 1

    total_lines = len(line_bins)
    ratio = numeric_line_count / total_lines if total_lines else 0.0
    return column_count, ratio


def _numeric_density(text: str) -> float:
    if not text:
        return 0.0
    digits = sum(1 for char in text if char.isdigit())
    return digits / len(text) if text else 0.0


def _collect_currency_tokens(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    if not text:
        return counts
    euro_symbol = text.count("\u20ac")
    if euro_symbol:
        counts["EUR_symbol"] = euro_symbol
    for token in _CURRENCY_TOKEN_RE.findall(text):
        if token in _KNOWN_CURRENCIES:
            counts[token] = counts.get(token, 0) + 1
    return counts
