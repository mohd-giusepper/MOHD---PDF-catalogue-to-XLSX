import hashlib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from pdf2xlsx import config
from pdf2xlsx.core import page_cache
from pdf2xlsx.io import triage_report
from pdf2xlsx.utils import labels as label_utils


ART_NO_LINE_RE = re.compile(r"\bArt\.?\s*no\.?\s*:", re.IGNORECASE)
PRICE_LINE_RE = re.compile(r"\b(?:EUR|DKK|SEK|NOK)\b.*\d", re.IGNORECASE)
HEADER_LINE_RE = re.compile(
    r"\b(code|description|descrizione|art\.?\s*no|price|prezzo|rrp)\b",
    re.IGNORECASE,
)


def compute_file_hash(path: Path) -> str:
    hasher = hashlib.sha1()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()[:8]


def sanitize_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._") or "pdf"


def build_run_id(timestamp: str, mode: str, input_hash: str) -> str:
    return f"{timestamp}_{mode}_{input_hash}"


def build_config_snapshot(target_currency: str) -> dict:
    return {
        "target_currency_default": target_currency,
        "currency_auto_min_ratio": config.CURRENCY_AUTO_MIN_RATIO,
        "currency_auto_min_count": config.CURRENCY_AUTO_MIN_COUNT,
        "triage_sample_pages_max": config.TRIAGE_SAMPLE_PAGES_MAX,
        "triage_text_len_min": config.TRIAGE_TEXT_LEN_MIN,
        "auto_rows_min": config.AUTO_ROWS_MIN,
        "auto_unique_code_ratio_min": config.AUTO_UNIQUE_CODE_RATIO_MIN,
        "auto_key_fields_rate_min": config.AUTO_KEY_FIELDS_RATE_MIN,
        "auto_excellent_key_fields_rate_min": config.AUTO_EXCELLENT_KEY_FIELDS_RATE_MIN,
        "auto_excellent_review_rate_max": config.AUTO_EXCELLENT_REVIEW_RATE_MAX,
        "review_rate_threshold": config.REVIEW_RATE_THRESHOLD,
    }


def build_input_manifest(paths: Iterable[Path]) -> Tuple[List[dict], str]:
    entries: List[dict] = []
    hasher = hashlib.sha1()
    for path in paths:
        size = path.stat().st_size
        pdf_hash = compute_file_hash(path)
        entries.append(
            {"name": path.name, "size": size, "pdf_hash": pdf_hash}
        )
        hasher.update(path.name.encode("utf-8"))
        hasher.update(str(size).encode("utf-8"))
        hasher.update(pdf_hash.encode("utf-8"))
    return entries, hasher.hexdigest()[:8]


def compute_summary(results: Iterable) -> dict:
    processed_ok = 0
    failed = 0
    skipped = 0
    bad_art_no_total = 0
    corrected_art_no_total = 0
    suspicious_numeric_seen = False
    for result in results:
        status = (result.final_status or "").upper()
        if status.startswith("CONVERTED") or status.startswith("PARTIAL"):
            processed_ok += 1
        elif status.startswith("FAILED"):
            failed += 1
        elif status.startswith("SKIPPED"):
            skipped += 1
        bad_art_no_total += int(getattr(result, "bad_art_no_count", 0) or 0)
        corrected_art_no_total += int(getattr(result, "corrected_art_no_count", 0) or 0)
        if getattr(result, "suspicious_numeric_art_no_seen", False):
            suspicious_numeric_seen = True
    return {
        "processed_ok": processed_ok,
        "failed": failed,
        "skipped": skipped,
        "bad_art_no_count": bad_art_no_total,
        "corrected_art_no_count": corrected_art_no_total,
        "suspicious_numeric_art_no_seen": suspicious_numeric_seen,
    }


class DebugPack:
    def __init__(
        self,
        base_dir: str,
        input_files: List[Path],
        mode: str,
        level: str = "light",
        target_currency: str = config.TARGET_CURRENCY,
    ) -> None:
        self.level = level
        self.mode = mode
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.input_files, input_hash = build_input_manifest(input_files)
        self.run_id = build_run_id(self.timestamp, mode, input_hash)
        self.root = Path(base_dir) / self.run_id
        self.root.mkdir(parents=True, exist_ok=True)
        self.start_time = time.monotonic()
        self.target_currency = target_currency
        self.partial_run = True
        self.write_manifest(summary={}, partial_run=True)

    def write_manifest(self, summary: dict, partial_run: bool) -> None:
        total_time_ms = int((time.monotonic() - self.start_time) * 1000)
        payload = {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "mode": self.mode,
            "partial_run": partial_run,
            "config_snapshot": build_config_snapshot(self.target_currency),
            "input_files": self.input_files,
            "summary_totals": summary,
            "total_time_ms": total_time_ms,
        }
        manifest_path = self.root / "run_manifest.json"
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2)

    def write_triage_report(self, results: List) -> None:
        triage_report.write_triage_report(results, str(self.root / "triage_report.xlsx"))
        json_path = self.root / "triage_report.json"
        with json_path.open("w", encoding="utf-8") as handle:
            json.dump(
                [result.to_dict() for result in results],
                handle,
                ensure_ascii=True,
                indent=2,
            )

    def write_pdf_pack(
        self,
        result,
        cached_pages: Optional[List[page_cache.CachedPage]] = None,
    ) -> None:
        pdf_dir = self.root / sanitize_name(Path(result.source_file).stem)
        pdf_dir.mkdir(parents=True, exist_ok=True)

        if cached_pages is None:
            cached_pages = self._build_cache(result.source_path)

        debug_payload = {
            "source_file": result.source_file,
            "pages_sampled": result.pages_sampled,
            "page_stats": [
                {
                    "page": page.page_number,
                    "text_len": page.text_len,
                    "images_count": page.images_count,
                    "needs_ocr": page.needs_ocr,
                    "ocr_used": page.ocr_used,
                }
                for page in cached_pages
            ],
            "signals": {
                "art_no_count": result.art_no_count,
                "rrp_count": result.rrp_count,
                "colli_count": result.colli_count,
                "designer_count": result.designer_count,
                "code_label_count": result.code_label_count,
                "description_label_count": result.description_label_count,
                "price_label_count": result.price_label_count,
                "price_count": result.price_count,
                "euro_count": result.euro_count,
                "currency_code_count": result.currency_code_count,
                "table_columns": result.table_columns,
                "numeric_line_ratio": result.numeric_line_ratio,
            },
            "scores": {
                "marker_score": result.marker_score,
                "table_score": result.table_score,
                "code_price_score": result.code_price_score,
                "support_score": result.support_score,
            },
            "selection_reason": result.selection_reason,
            "final_status": result.final_status,
            "winner_parser": result.winner_parser,
            "failure_reason": result.failure_reason,
            "target_currency": result.target_currency,
            "currency_confidence": result.currency_confidence,
            "currency_counts": result.currency_counts,
            "bad_art_no_count": result.bad_art_no_count,
            "corrected_art_no_count": result.corrected_art_no_count,
            "suspicious_numeric_art_no_seen": result.suspicious_numeric_art_no_seen,
            "examples_bad_art_no": result.examples_bad_art_no,
        }

        debug_path = pdf_dir / "debug.json"
        with debug_path.open("w", encoding="utf-8") as handle:
            json.dump(debug_payload, handle, ensure_ascii=True, indent=2)

        snippets = collect_snippets(cached_pages, result.failure_reason)
        snippets_path = pdf_dir / "raw_snippets.txt"
        with snippets_path.open("w", encoding="utf-8") as handle:
            handle.write("\n".join(snippets))

        attempts_path = pdf_dir / "attempts_detail.json"
        with attempts_path.open("w", encoding="utf-8") as handle:
            json.dump(
                result.attempts_detail or [],
                handle,
                ensure_ascii=True,
                indent=2,
            )

        if self.level == "full":
            self._write_preview_pages(result.source_path, result.pages_sampled, pdf_dir)

    def finalize(self, results: List, partial_run: bool) -> None:
        summary = compute_summary(results)
        self.write_manifest(summary=summary, partial_run=partial_run)

    def _build_cache(self, pdf_path: str) -> List[page_cache.CachedPage]:
        label_dict = label_utils.load_label_dictionary()
        stopwords = label_dict.get("stopwords") or config.TRIAGE_STOPWORDS
        cached_pages, _ = page_cache.build_sample_cache(
            pdf_path,
            max_pages=config.TRIAGE_SAMPLE_PAGES_MAX,
            min_text_len=config.TRIAGE_TEXT_LEN_MIN,
            stopwords=stopwords,
            ocr=False,
        )
        return cached_pages

    def _write_preview_pages(
        self, pdf_path: str, pages_sampled: List[int], pdf_dir: Path
    ) -> None:
        try:
            from pdf2image import convert_from_path
        except Exception:
            return

        if not pages_sampled:
            pages_sampled = [1]
        preview_pages = pages_sampled[:2]
        preview_dir = pdf_dir / "preview_pages"
        preview_dir.mkdir(parents=True, exist_ok=True)
        for page_number in preview_pages:
            try:
                images = convert_from_path(
                    pdf_path,
                    first_page=page_number,
                    last_page=page_number,
                )
            except Exception:
                continue
            if not images:
                continue
            output_path = preview_dir / f"page_{page_number}.png"
            try:
                images[0].save(output_path)
            except Exception:
                continue


def collect_snippets(
    cached_pages: List[page_cache.CachedPage],
    failure_reason: str,
    max_lines: int = 200,
) -> List[str]:
    art_lines: List[str] = []
    price_lines: List[str] = []
    header_lines: List[str] = []
    failure_lines: List[str] = []

    for page in cached_pages:
        for line in page.lines:
            clean = (line or "").strip()
            if not clean:
                continue
            if len(art_lines) < 50 and ART_NO_LINE_RE.search(clean):
                art_lines.append(clean)
                continue
            if len(price_lines) < 50 and PRICE_LINE_RE.search(clean):
                price_lines.append(clean)
                continue
            if len(header_lines) < 50 and HEADER_LINE_RE.search(clean):
                header_lines.append(clean)
                continue
            if failure_reason and len(failure_lines) < 3 and re.search(r"\d", clean):
                failure_lines.append(clean)

    output: List[str] = []
    output.extend(["[art_no_lines]"] + art_lines)
    output.extend(["", "[price_lines]"] + price_lines)
    output.extend(["", "[header_lines]"] + header_lines)
    if failure_lines:
        output.extend(["", "[failure_examples]"] + failure_lines)

    return output[:max_lines]
