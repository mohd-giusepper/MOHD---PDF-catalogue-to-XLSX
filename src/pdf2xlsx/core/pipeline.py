import hashlib
import logging
import os
import re
from pathlib import Path
from collections import Counter, defaultdict
from typing import Callable, Dict, List, Optional, Tuple

from pdf2xlsx import config
from pdf2xlsx.core import extract, normalize, scoring
from pdf2xlsx.io import json_debug, xlsx_writer
from pdf2xlsx.models import ProductRow, RunReport
from pdf2xlsx.parsers import get_parser
from pdf2xlsx.utils import text as text_utils


LOGGER = logging.getLogger(__name__)
ART_NO_LINE_RE = re.compile(r"Art\.?\s*no\.?\s*:", re.IGNORECASE)
ART_NO_VALUE_RE = re.compile(
    r"Art\.?\s*no\.?\s*:\s*([A-Z0-9]+(?:-[A-Z0-9]+)*)",
    re.IGNORECASE,
)
LARGE_NUMBER_RE = re.compile(r"\b\d{6,}\b")
DISCARD_SAMPLE_LIMIT = 5
CODE_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9\-./]*")
MULTI_NUMBER_RE = re.compile(r"\d+(?:[.,]\d+)?")
COLUMN_SPLIT_RE = re.compile(r"\s{2,}|\t")
LEGAL_MARKER_PATTERNS = [
    re.compile(r"\bcgv\b", re.IGNORECASE),
    re.compile(r"\bterms\b", re.IGNORECASE),
    re.compile(r"\bconditions\b", re.IGNORECASE),
    re.compile(r"\bterms and conditions\b", re.IGNORECASE),
    re.compile(r"\bcondizioni generali\b", re.IGNORECASE),
    re.compile(r"\bdecreto legislativo\b", re.IGNORECASE),
    re.compile(r"\bdirettiva\b", re.IGNORECASE),
    re.compile(r"\bcodice civile\b", re.IGNORECASE),
    re.compile(r"\bwarranty\b", re.IGNORECASE),
    re.compile(r"\bgaranzia\b", re.IGNORECASE),
    re.compile(r"\bliability\b", re.IGNORECASE),
    re.compile(r"responsabilita\b", re.IGNORECASE),
    re.compile(r"responsabilit\u00e0\b", re.IGNORECASE),
    re.compile(r"\bjurisdiction\b", re.IGNORECASE),
    re.compile(r"\bgoverning law\b", re.IGNORECASE),
    re.compile(r"\barticolo\b", re.IGNORECASE),
    re.compile(r"\bart\.(?!\s*no)", re.IGNORECASE),
    re.compile(r"\b(?:ec|ue)\b", re.IGNORECASE),
    re.compile(r"\u00a7", re.IGNORECASE),
]
DIM_LABEL_RE = re.compile(
    r"\b(prof\.?|larg\.?|alt\.?|profondit[aà]|larghezza|altezza|diametro|dia|diam|depth|width|height|h|w|d|l|o|\u00f8|ø)\b",
    re.IGNORECASE,
)
HEADER_STRICT_KEYWORDS = [
    "listino",
    "legenda",
    "cod.",
    "codice",
    "code",
    "descrizione",
    "description",
    "price",
    "prezzo",
    "rrp",
    "art. no",
    "art no",
    "designer",
    "colli",
    "size",
    "dimensioni",
    "dimensions",
]


def run_pipeline(
    input_pdf: str,
    output_xlsx: str,
    pages: Optional[List[int]] = None,
    debug_json: Optional[str] = None,
    parser_name: Optional[str] = None,
    debug_blocks: int = 0,
    currency_only: Optional[str] = None,
    filter_currency: bool = True,
    ocr: bool = False,
    debug_matches: bool = False,
    allow_empty_output: bool = True,
    progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
) -> RunReport:
    parser = get_parser(parser_name or config.DEFAULT_PARSER)
    source_file = os.path.basename(input_pdf)
    blocks = []
    rows: List[ProductRow] = []
    debug_items: List[dict] = []
    current_section = ""
    page_stats: List[dict] = []
    debug_art_no_samples = 20
    target_currency = (currency_only or config.TARGET_CURRENCY).upper()
    skipped_missing_target_price = 0
    rows_candidate = 0
    rows_after_parsing = 0
    rows_after_filters = 0
    discard_reasons: Counter = Counter()
    discard_samples: Dict[str, List[str]] = defaultdict(list)
    cooccurrence_samples: List[str] = []

    cleanup_output_dir(output_xlsx, debug_json)

    for page in extract.extract_pages(
        input_pdf, pages, ocr=ocr, progress_callback=progress_callback
    ):
        page_stats.append(
            {
                "page": page.page_number,
                "text_len": page.text_len,
                "images_count": page.images_count,
                "needs_ocr": page.needs_ocr,
                "ocr_used": page.ocr_used,
            }
        )
        if page.needs_ocr and not page.ocr_used and ocr is False:
            LOGGER.info("Page %d flagged for OCR (skipped).", page.page_number)
        if not page.text:
            LOGGER.warning("Empty text on page %d", page.page_number)
            continue
        normalized = normalize.normalize_text(page.text)
        if debug_matches and page.page_number in {2, 3}:
            log_marker_debug(page.page_number, normalized)
        lines = normalize.split_lines(normalized)
        page_signals = compute_page_signals(lines)
        LOGGER.info(
            "PAGE_SIG p=%d parser=%s sig=%s",
            page.page_number,
            parser.name,
            format_page_signals(page_signals),
        )
        if is_text_like_page(page_signals, parser.name):
            LOGGER.warning(
                "SKIP_PAGE text_like p=%d parser=%s sig=%s",
                page.page_number,
                parser.name,
                format_page_signals(page_signals),
            )
            continue
        page_review_only = is_review_only_page(page_signals)
        if page_review_only:
            LOGGER.warning(
                "REVIEW_PAGE legal_weak_table p=%d parser=%s sig=%s",
                page.page_number,
                parser.name,
                format_page_signals(page_signals),
            )

        section = parser.detect_section(lines)
        if section:
            current_section = section
            lines = normalize.strip_section_line(lines, section)

        page_blocks = parser.segment_blocks(lines)
        for block_text in page_blocks:
            notes = []
            has_art_no = parser.contains_art_no(block_text)
            has_price = bool(parser.price_pattern.search(block_text))
            if not has_art_no and has_price:
                if blocks and blocks[-1]["page"] in {page.page_number, page.page_number - 1}:
                    blocks[-1]["raw_text"] += "\n" + block_text
                    blocks[-1]["notes"].append("continuation_attached")
                    continue
                notes.append("orphan_block_no_art_no")
            blocks.append(
                {
                    "page": page.page_number,
                    "section": current_section,
                    "raw_text": block_text,
                    "ocr_used": page.ocr_used,
                    "notes": notes,
                    "review_only": page_review_only,
                }
            )

    LOGGER.info("Blocks detected: %d", len(blocks))

    if debug_blocks > 0:
        print_debug_blocks(blocks, debug_blocks)

    for block in blocks:
        sub_blocks, merged_note = parser.split_merged_block(block["raw_text"])
        if merged_note:
            LOGGER.warning("Possible merged block on page %s", block.get("page"))

        for sub_block in sub_blocks:
            row, size_unparsed, parse_notes = parser.parse_block(
                raw_text=sub_block,
                page=block["page"],
                section=block["section"],
                source_file=source_file,
            )
            rows_candidate += 1
            rows_after_parsing += 1
            normalized_block = normalize.normalize_text(sub_block)
            flat_line = " ".join(normalized_block.split())
            line_info = text_utils.analyze_line(flat_line)
            token_info = text_utils.resolve_row_fields(flat_line)
            selected_art_no = token_info.get("selected_art_no") or {}
            if not row.art_no and selected_art_no:
                art_no_value = selected_art_no.get("token") if isinstance(selected_art_no, dict) else ""
                if art_no_value and not token_info.get("ambiguous_numeric"):
                    row.art_no = art_no_value
                    row.art_no_raw = art_no_value
            if not row.product_name_en and token_info.get("name"):
                row.product_name_en = token_info.get("name") or ""
            if not row.size_raw and token_info.get("dimension_candidates"):
                row.size_raw = " | ".join(token_info.get("dimension_candidates") or [])
            discard_reason = primary_discard_reason(
                parser=parser,
                row=row,
                raw_text=flat_line,
                line_info=line_info,
            )
            if discard_reason:
                if discard_reason == "bad_id" and (
                    token_info.get("ambiguous_numeric")
                    or token_info.get("art_no_candidates")
                    or token_info.get("price_candidates")
                ):
                    discard_reason = ""
                if discard_reason:
                    discard_reasons[discard_reason] += 1
                    if len(discard_samples[discard_reason]) < DISCARD_SAMPLE_LIMIT:
                        discard_samples[discard_reason].append(flat_line[:200])
                    continue
            confidence, needs_review, notes = scoring.score_row(
                row=row,
                size_unparsed=size_unparsed,
                ocr_used=block.get("ocr_used", False),
                currency=target_currency,
            )

            raw_block_id = hashlib.sha1(
                f"{block['page']}:{normalized_block}".encode("utf-8")
            ).hexdigest()
            raw_snippet = ""

            row.confidence = confidence
            row.needs_review = needs_review
            combined_notes = parse_notes + notes + block.get("notes", [])
            if merged_note:
                combined_notes.append(merged_note)
            if token_info.get("dimension_candidates"):
                combined_notes.append("dim_detected")
            if token_info.get("price_candidates"):
                combined_notes.append("price_detected")
            if token_info.get("art_no_candidates"):
                combined_notes.append("artno_detected")
            if token_info.get("ambiguous_numeric"):
                row.exported = False
                row.needs_review = True
                combined_notes.append("ambiguous_price_vs_artno")
            if block.get("review_only"):
                row.exported = False
                row.needs_review = True
                combined_notes.append("page_review_only")
            art_line = ""
            matched_art_no = ""
            first_large_number_token = ""
            for line in sub_block.splitlines():
                if ART_NO_LINE_RE.search(line):
                    art_line = line.strip()
                    match = ART_NO_VALUE_RE.search(line)
                    if match:
                        matched_art_no = match.group(1).strip()
                    large_match = LARGE_NUMBER_RE.search(line)
                    if large_match:
                        first_large_number_token = large_match.group(0)
                    break

            snippet_source = art_line or flat_line
            raw_snippet = snippet_source[:200]
            row.raw_block_id = raw_block_id
            row.raw_snippet = raw_snippet

            if (
                row.art_no
                and row.art_no.isdigit()
                and len(row.art_no) >= 6
                and matched_art_no
                and matched_art_no != row.art_no
            ):
                combined_notes.append("art_no_corrected")
                LOGGER.info(
                    "art_no_corrected raw=%s matched=%s line=%s",
                    row.art_no,
                    matched_art_no,
                    art_line,
                )
                row.art_no_raw = matched_art_no
                row.art_no = matched_art_no

            if row.art_no:
                row.art_no = text_utils.canonicalize_art_no(row.art_no)
            if debug_art_no_samples > 0:
                source_line = ""
                for line in sub_block.splitlines():
                    if line.strip():
                        source_line = line.strip()
                        break
                art_no_canonical = canonical_art_no_key(
                    row.art_no, art_line or row.raw_snippet or ""
                )
                starts_numeric = bool(re.match(r"\d", art_no_canonical))
                LOGGER.info(
                    "art_no_sample raw=%s canonical=%s matched_art_no=%s first_large_number_token=%s starts_numeric=%s name=%s source_line=%s",
                    row.art_no_raw or "",
                    art_no_canonical,
                    matched_art_no or "",
                    first_large_number_token or "",
                    starts_numeric,
                    row.product_name_en or "",
                    source_line,
                )
                debug_art_no_samples -= 1
            if row.art_no and line_info.get("price_like"):
                if len(cooccurrence_samples) < DISCARD_SAMPLE_LIMIT:
                    cooccurrence_samples.append(raw_snippet)

            invalid_price_note = f"invalid_price_{target_currency.lower()}"
            force_review = merged_note in {"merged_block_unsplit", "possible_merged_block"}
            if invalid_price_note in parse_notes:
                force_review = True

            art_no_count = len(parser.art_no_regex.findall(sub_block))
            if parser.name in {"table_based", "code_price_based"}:
                art_no_count = 1 if row.art_no else 0
            has_any_price = bool(parser.price_pattern.search(sub_block))
            price_regex = getattr(parser, "price_regex", None)
            if price_regex and price_regex.search(sub_block):
                has_any_price = True
            if line_info.get("price_like"):
                has_any_price = True
            if row_has_price(row):
                has_any_price = True
            invalid_reasons = []
            if not row.art_no:
                invalid_reasons.append("invalid_chunk_missing_art_no")
            elif art_no_count > 1 and parser.name not in {"table_based", "code_price_based"}:
                invalid_reasons.append("invalid_chunk_multiple_art_no")
            if not has_any_price:
                invalid_reasons.append("invalid_chunk_missing_price")
            if not row.product_name_en:
                invalid_reasons.append("invalid_chunk_missing_name")
            if not token_info.get("price_candidates") and not token_info.get("art_no_candidates"):
                invalid_reasons.append("no_price_no_artno")
            if invalid_reasons:
                row.exported = False
                row.needs_review = True
                combined_notes.extend(invalid_reasons)

            hard_ok, hard_reason = hard_validate_row(
                row=row,
                raw_text=flat_line,
                line_info=line_info,
                token_info=token_info,
            )
            if not hard_ok:
                row.exported = False
                row.needs_review = True
                combined_notes.append(hard_reason)
                if hard_reason in {"hard_legal", "dim_only_row", "header_like_strict"}:
                    row.confidence = min(row.confidence, 0.3)
                LOGGER.warning(
                    "HARD_FAIL_ROW reason=%s art_no=%s page=%s parser=%s snippet=%s",
                    hard_reason,
                    row.art_no or "",
                    row.page or "",
                    parser.name,
                    flat_line[:160],
                )

            if is_degraded_art_no(sub_block, row.art_no):
                combined_notes.append("art_no_degraded")
                row.needs_review = True

            if any(
                reason in combined_notes
                for reason in ("hard_legal", "dim_only_row", "header_like_strict")
            ):
                row.confidence = min(row.confidence, 0.3)
                row.needs_review = True
                row.exported = False

            row.notes = "; ".join(unique_notes(combined_notes))
            if force_review:
                row.needs_review = True

            if filter_currency:
                apply_currency_filter(row, target_currency)
                if get_price_by_currency(row, target_currency) is None:
                    skipped_missing_target_price += 1
                    if skipped_missing_target_price <= 5:
                        LOGGER.info(
                            "Skipping row missing target currency price: currency=%s art_no=%s page=%s",
                            target_currency,
                            row.art_no or "",
                            row.page,
                        )
                    continue

            rows.append(row)
            rows_after_filters += 1

            if debug_json:
                debug_items.append(
                    {
                        "page": block["page"],
                        "section": block["section"],
                        "ocr_used": block.get("ocr_used", False),
                        "raw_block_id": raw_block_id,
                        "raw_snippet": raw_snippet,
                        "raw_text": sub_block,
                        "row_index": len(rows) - 1,
                    }
                )

    dedup_info: Dict[str, object] = {}
    apply_duplicate_policy(rows, target_currency, dedup_info)
    ensure_review_for_missing_prices(rows, target_currency)

    if debug_json:
        for item in debug_items:
            row_index = item.pop("row_index", None)
            if isinstance(row_index, int) and 0 <= row_index < len(rows):
                item["parsed"] = rows[row_index].to_dict()
        json_debug.write_debug_json(
            debug_json, {"pages": page_stats, "blocks": debug_items}
        )

    exported_rows = [row for row in rows if row.exported]
    if allow_empty_output or exported_rows:
        temp_path = f"{output_xlsx}.tmp"
        try:
            xlsx_writer.write_xlsx(rows, temp_path)
            Path(output_xlsx).parent.mkdir(parents=True, exist_ok=True)
            Path(output_xlsx).unlink(missing_ok=True)
            Path(temp_path).replace(output_xlsx)
        except OSError:
            LOGGER.warning("Failed to write XLSX output for %s", output_xlsx)
    else:
        LOGGER.warning("No exported rows; skipping XLSX write for %s", output_xlsx)
        try:
            output_path = Path(output_xlsx)
            if output_path.exists():
                output_path.unlink()
        except OSError:
            LOGGER.warning("Could not remove empty XLSX output: %s", output_xlsx)

    needs_review_count = sum(1 for row in rows if row.needs_review)
    if skipped_missing_target_price:
        LOGGER.info(
            "Skipped rows missing %s price: %d",
            target_currency,
            skipped_missing_target_price,
        )
    LOGGER.info("Rows written: %d", len(rows))
    if rows:
        LOGGER.info(
            "Needs review: %d (%.1f%%)",
            needs_review_count,
            100.0 * needs_review_count / len(rows),
        )
    LOGGER.info("Output saved to %s", output_xlsx)

    return build_report(
        rows,
        page_stats,
        target_currency,
        skipped_missing_target_price,
        rows_candidate,
        rows_after_parsing,
        rows_after_filters,
        discard_reasons,
        discard_samples,
        dedup_info,
        cooccurrence_samples,
    )


def build_report(
    rows: List[ProductRow],
    page_stats: List[dict],
    target_currency: str,
    skipped_missing_target_price: int,
    rows_candidate: int,
    rows_after_parsing: int,
    rows_after_filters: int,
    discard_reasons: Counter,
    discard_samples: Dict[str, List[str]],
    dedup_info: Dict[str, object],
    cooccurrence_samples: List[str],
) -> RunReport:
    target_currency = (target_currency or config.TARGET_CURRENCY).upper()
    exported_rows = [row for row in rows if row.exported]
    missing_price = sum(
        1
        for row in exported_rows
        if get_price_by_currency(row, target_currency) is None
    )

    duplicate_summary = analyze_duplicates(rows, target_currency)
    art_no_quality = analyze_art_no_quality(rows)
    review_reasons = analyze_review_reasons(rows)

    report = RunReport(
        rows=rows,
        pages_processed=len(page_stats),
        pages_needing_ocr=sum(1 for p in page_stats if p.get("needs_ocr")),
        pages_ocr_used=sum(1 for p in page_stats if p.get("ocr_used")),
        rows_needs_review=sum(1 for row in rows if row.needs_review),
        missing_art_no=sum(1 for row in rows if not row.art_no),
        missing_price=missing_price,
        skipped_missing_target_price=skipped_missing_target_price,
        rows_exported=len(exported_rows),
        duplicate_art_no_count=duplicate_summary["count"],
        duplicate_art_no_top=duplicate_summary["top"],
        duplicate_conflicts=duplicate_summary["conflicts"],
        duplicate_conflicts_count=duplicate_summary["conflicts_count"],
        bad_art_no_count=art_no_quality["bad_art_no_count"],
        corrected_art_no_count=art_no_quality["corrected_art_no_count"],
        suspicious_numeric_art_no_seen=art_no_quality["suspicious_numeric_art_no_seen"],
        examples_bad_art_no=art_no_quality["examples_bad_art_no"],
        review_reasons_top=review_reasons,
        target_currency=target_currency,
        examples_ok=[row for row in rows if not row.needs_review][:3],
        examples_needs_review=[row for row in rows if row.needs_review][:3],
        page_stats=page_stats,
        rows_candidate=rows_candidate,
        rows_after_parsing=rows_after_parsing,
        rows_after_filters=rows_after_filters,
        discard_reasons=dict(discard_reasons),
        discard_samples=discard_samples,
        duplicates_summary=dedup_info.get("duplicates_summary", []),
        cooccurrence_samples=cooccurrence_samples,
        config_info={
            "threshold_text_len_for_ocr": config.THRESHOLD_TEXT_LEN_FOR_OCR,
            "confidence_threshold": config.CONFIDENCE_THRESHOLD,
            "price_min": config.PRICE_MIN,
            "price_max": config.PRICE_MAX,
            "review_rate_threshold": config.REVIEW_RATE_THRESHOLD,
        },
    )
    if art_no_quality["bad_art_no_count"] > 0:
        LOGGER.warning(
            "Bad art_no detected: count=%d suspicious_numeric=%s",
            art_no_quality["bad_art_no_count"],
            art_no_quality["suspicious_numeric_art_no_seen"],
        )
        for example in art_no_quality["examples_bad_art_no"]:
            LOGGER.warning("bad_art_no_example: %s", example)
    return report


def analyze_duplicates(rows: List[ProductRow], target_currency: str) -> dict:
    art_nos = [
        key
        for row in rows
        if (key := canonical_art_no_key(row.art_no, row.raw_snippet or ""))
    ]
    counter = Counter(art_nos)
    duplicates = [(art_no, count) for art_no, count in counter.items() if count > 1]
    duplicates.sort(key=lambda item: (-item[1], item[0]))

    conflicts = []
    grouped = defaultdict(list)
    for row in rows:
        key = canonical_art_no_key(row.art_no, row.raw_snippet or "")
        if key:
            grouped[key].append(row)

    for art_no, group_rows in grouped.items():
        if len(group_rows) < 2:
            continue
        values = set(
            (
                row.product_name_en.strip().lower(),
                get_price_by_currency(row, target_currency),
            )
            for row in group_rows
        )
        if len(values) > 1:
            conflicts.append(art_no)
            log_duplicate_conflict(art_no, group_rows, len(values))

    return {
        "count": len(duplicates),
        "top": duplicates[:10],
        "conflicts": conflicts,
        "conflicts_count": len(conflicts),
    }


def analyze_art_no_quality(rows: List[ProductRow]) -> dict:
    bad_count = 0
    corrected_count = 0
    suspicious_seen = False
    examples = []

    for row in rows:
        if "art_no_corrected" in (row.notes or ""):
            corrected_count += 1
        raw_text = row.raw_snippet or row.product_name_raw or ""
        match = ART_NO_VALUE_RE.search(raw_text)
        matched_art_no = match.group(1).strip() if match else ""
        canonical = text_utils.canonicalize_art_no(row.art_no or "")
        matched_canonical = text_utils.canonicalize_art_no(matched_art_no)
        large_match = LARGE_NUMBER_RE.search(raw_text)
        large_token = large_match.group(0) if large_match else ""
        suspicious_numeric = canonical.isdigit() and len(canonical) >= 6 and match
        mismatch = bool(match and canonical and matched_canonical and canonical != matched_canonical)
        if suspicious_numeric:
            suspicious_seen = True
        if suspicious_numeric or mismatch:
            bad_count += 1
            if len(examples) < 5:
                examples.append(
                    {
                        "art_no": canonical,
                        "matched_art_no": matched_canonical,
                        "first_large_number_token": large_token,
                        "page": row.page or "",
                    }
                )

    return {
        "bad_art_no_count": bad_count,
        "corrected_art_no_count": corrected_count,
        "suspicious_numeric_art_no_seen": suspicious_seen,
        "examples_bad_art_no": examples,
    }


def cleanup_output_dir(output_xlsx: str, debug_json: Optional[str]) -> None:
    output_root = Path(config.OUTPUT_DIR).resolve()
    output_path = Path(output_xlsx).resolve()
    target_dir = output_path.parent

    if output_root not in [target_dir, *target_dir.parents]:
        return

    keep = {output_path}
    if debug_json:
        keep.add(Path(debug_json).resolve())

    stem = output_path.stem
    patterns = [f"{stem}*.xlsx", f"{stem}*{config.DEBUG_JSON_SUFFIX}"]
    for pattern in patterns:
        for path in target_dir.glob(pattern):
            if path.resolve() in keep:
                continue
            try:
                path.unlink()
                LOGGER.info("Removed old output file: %s", path)
            except OSError:
                LOGGER.warning("Could not remove old output file: %s", path)


def analyze_review_reasons(rows: List[ProductRow]) -> List[tuple]:
    counter: Counter = Counter()
    for row in rows:
        if not row.needs_review and row.exported:
            continue
        if not row.notes:
            counter["unspecified"] += 1
            continue
        for note in row.notes.split(";"):
            reason = note.strip()
            if reason:
                counter[reason] += 1
    return counter.most_common(5)


def unique_notes(notes: List[str]) -> List[str]:
    seen = set()
    unique = []
    for note in notes:
        clean = note.strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        unique.append(clean)
    return unique


def row_has_price(row: ProductRow) -> bool:
    return any(
        price is not None
        for price in (row.price_eur, row.price_dkk, row.price_sek, row.price_nok)
    )


def compute_page_signals(lines: List[str]) -> Dict[str, object]:
    text = " ".join(lines)
    numeric_density = _numeric_density(text)
    price_like_count = 0
    cooccurrence_count = 0
    for line in lines:
        if not line:
            continue
        line_info = text_utils.analyze_line(line)
        price_like = bool(line_info.get("price_like"))
        if price_like:
            price_like_count += 1
        if price_like and _line_has_plausible_code(line):
            cooccurrence_count += 1
    legal_hits = count_legal_markers(text)
    table_hint = _table_hint_from_lines(lines)
    table_columns = estimate_table_columns(lines)
    return {
        "numeric_density": numeric_density,
        "price_like_count": price_like_count,
        "cooccurrence_count": cooccurrence_count,
        "legal_hits": legal_hits,
        "table_hint": table_hint,
        "table_columns": table_columns,
    }


def format_page_signals(signals: Dict[str, object]) -> str:
    return (
        f"density={signals.get('numeric_density', 0.0):.3f} "
        f"price_like={signals.get('price_like_count', 0)} "
        f"cooc={signals.get('cooccurrence_count', 0)} "
        f"legal={signals.get('legal_hits', 0)} "
        f"table_hint={bool(signals.get('table_hint'))} "
        f"table_cols={signals.get('table_columns', 0)}"
    )


def is_text_like_page(signals: Dict[str, object], parser_name: str) -> bool:
    cooc = int(signals.get("cooccurrence_count", 0) or 0)
    price_like_count = int(signals.get("price_like_count", 0) or 0)
    numeric_density = float(signals.get("numeric_density", 0.0) or 0.0)
    legal_hits = int(signals.get("legal_hits", 0) or 0)
    table_hint = bool(signals.get("table_hint"))
    table_columns = int(signals.get("table_columns", 0) or 0)

    if cooc == 0 and price_like_count < 2 and numeric_density < 0.08:
        return True
    if legal_hits >= 2 and cooc == 0 and price_like_count < 3:
        return True
    if parser_name == "table_based" and not table_hint and table_columns < 3:
        return True
    return False


def is_review_only_page(signals: Dict[str, object]) -> bool:
    legal_hits = int(signals.get("legal_hits", 0) or 0)
    cooc = int(signals.get("cooccurrence_count", 0) or 0)
    table_hint = bool(signals.get("table_hint"))
    if legal_hits >= 2 and (cooc < 2 or not table_hint):
        return True
    return False


def hard_validate_row(
    row: ProductRow,
    raw_text: str,
    line_info: Dict[str, object],
    token_info: Optional[Dict[str, object]] = None,
) -> Tuple[bool, str]:
    if looks_like_legal_text(raw_text):
        return False, "hard_legal"
    if token_info and is_dimension_only_row(raw_text, token_info):
        return False, "dim_only_row"
    if is_header_like_strict(raw_text):
        return False, "header_like_strict"
    art_no = text_utils.canonicalize_art_no(row.art_no or "")
    if not art_no:
        return False, "hard_art_no"
    has_alpha = bool(re.search(r"[A-Za-z]", art_no))
    has_digit = bool(re.search(r"\d", art_no))
    digits_only = re.sub(r"\D", "", art_no)
    if not has_digit:
        return False, "hard_art_no_shape"
    if not has_alpha:
        if token_info and token_info.get("ambiguous_numeric"):
            return False, "ambiguous_price_vs_artno"
        if len(digits_only) < 3 or len(digits_only) > 10:
            return False, "hard_art_no_len"
    if has_alpha and (len(art_no) < 4 or len(art_no) > 24):
        return False, "hard_art_no_len"

    if not row_has_price(row):
        return False, "hard_no_price"

    name = (row.product_name_en or "").strip()
    if not name:
        return False, "hard_no_name"
    if len(name) < 3 or len(name) > 90:
        return False, "hard_name_len"
    alpha_count = sum(1 for char in name if char.isalpha())
    if alpha_count < 3:
        return False, "hard_name_alpha"

    return True, ""


def looks_like_legal_text(raw_text: str) -> bool:
    if not raw_text:
        return False
    hits = count_legal_markers(raw_text)
    if hits >= 2:
        return True
    if hits >= 1 and len(raw_text) > 220:
        punct_hits = sum(raw_text.count(token) for token in (";", ":", "\u00a7"))
        if punct_hits >= 2:
            return True
    return False


def count_legal_markers(raw_text: str) -> int:
    if not raw_text:
        return 0
    hits = 0
    for pattern in LEGAL_MARKER_PATTERNS:
        if pattern.search(raw_text):
            hits += 1
    return hits


def is_dimension_only_row(raw_text: str, token_info: Dict[str, object]) -> bool:
    if not raw_text:
        return False
    name = token_info.get("name") or ""
    raw_name_tokens = [token for token in name.split() if re.search(r"[A-Za-z]", token)]
    name_tokens = [token for token in raw_name_tokens if not DIM_LABEL_RE.search(token)]
    name_alpha_count = sum(
        1 for token in name_tokens for char in token if char.isalpha()
    )
    has_dim_candidate = bool(token_info.get("dimension_candidates"))
    has_dim_label = bool(DIM_LABEL_RE.search(raw_text))
    if not (has_dim_candidate or has_dim_label):
        return False
    if len(name_tokens) >= 2:
        return False
    return name_alpha_count < 4


def is_header_like_strict(raw_text: str) -> bool:
    if not raw_text:
        return False
    lowered = raw_text.lower()
    hits = sum(1 for keyword in HEADER_STRICT_KEYWORDS if keyword in lowered)
    if hits >= 2:
        return True
    if hits >= 1 and len(raw_text) <= 60:
        return True
    return False


def _line_has_plausible_code(line: str) -> bool:
    for token in CODE_TOKEN_RE.findall(line or ""):
        if text_utils.is_plausible_code(token, min_len=config.CODE_MIN_LEN):
            return True
    return False


def _numeric_density(text: str) -> float:
    if not text:
        return 0.0
    digits = sum(1 for char in text if char.isdigit())
    return digits / len(text) if text else 0.0


def _table_hint_from_lines(lines: List[str]) -> bool:
    if not lines:
        return False
    numeric_lines = 0
    spaced_lines = 0
    for line in lines:
        if not line:
            continue
        if len(MULTI_NUMBER_RE.findall(line)) >= 2:
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


def estimate_table_columns(lines: List[str]) -> int:
    max_columns = 0
    for line in lines:
        if not line:
            continue
        parts = [part.strip() for part in COLUMN_SPLIT_RE.split(line) if part.strip()]
        if len(parts) > max_columns:
            max_columns = len(parts)
    return max_columns


def primary_discard_reason(parser, row: ProductRow, raw_text: str, line_info: Dict[str, object]) -> str:
    if not row.art_no:
        return "bad_id"
    header_like_fn = getattr(parser, "_is_header_like", None)
    if header_like_fn and header_like_fn(raw_text):
        return "header_like"
    if line_info.get("dimension_line") and not line_info.get("price_like"):
        return "dimension_line"
    if parser.name == "code_price_based" and not line_info.get("price_like"):
        return "no_price"
    if not line_info.get("price_like") and not row_has_price(row):
        return "no_price"
    return ""


def canonical_art_no_key(value: str, raw_text: str = "") -> str:
    key = text_utils.canonicalize_art_no(value or "")
    if not key:
        return ""
    if not re.search(r"\d", key):
        return ""
    if key.isdigit() and len(key) >= 6 and raw_text and ART_NO_LINE_RE.search(raw_text):
        return ""
    return key


def log_duplicate_conflict(duplicate_key: str, rows: List[ProductRow], variants: int) -> None:
    LOGGER.warning("Duplicate art_no conflict: key=%s variants=%d", duplicate_key, variants)
    for sample in rows[:3]:
        LOGGER.warning(
            "duplicate_sample art_no=%s name=%s page=%s raw_line=%s",
            sample.art_no or "",
            sample.product_name_en or "",
            sample.page or "",
            sample.raw_snippet or sample.product_name_raw or "",
        )


def is_degraded_art_no(raw_text: str, art_no: str) -> bool:
    if not art_no or "-" in art_no:
        return False
    if not art_no.isdigit():
        return False
    if len(art_no) < 4:
        return False
    return bool(
        re.search(
            r"Art\.?\s*no\.?\s*:\s*\d+\s*[-\u00ad]\s*\d+",
            raw_text,
            re.IGNORECASE,
        )
    )


def ensure_review_for_missing_prices(rows: List[ProductRow], currency: str) -> None:
    currency = currency.upper()
    for row in rows:
        if not row.exported:
            continue
        if get_price_by_currency(row, currency) is None:
            add_row_note(row, f"missing_price_{currency.lower()}")
            row.needs_review = True


def apply_duplicate_policy(
    rows: List[ProductRow], currency: str, dedup_info: Optional[Dict[str, object]] = None
) -> None:
    currency = currency.upper()
    grouped = defaultdict(list)
    for row in rows:
        key = canonical_art_no_key(row.art_no, row.raw_snippet or "")
        if key:
            row.art_no = key
            grouped[key].append(row)

    summary_items = []
    for art_no, group_rows in grouped.items():
        if len(group_rows) < 2:
            continue

        scored = []
        for row in group_rows:
            line_text = row.raw_snippet or row.product_name_raw or ""
            line_info = text_utils.analyze_line(line_text)
            score = row_score(row, currency, line_info=line_info)
            price_like = bool(line_info.get("price_like")) or row_has_price(row)
            scored.append(
                {
                    "row": row,
                    "score": score,
                    "line_info": line_info,
                    "price_like": price_like,
                    "line_text": line_text,
                }
            )

        scored.sort(key=lambda item: item["score"], reverse=True)
        best_row = scored[0]["row"]
        variant_reasons = Counter()
        variant_samples = []

        for item in scored[1:]:
            row = item["row"]
            line_info = item["line_info"]
            price_like = item["price_like"]
            if line_info.get("dimension_line") and not price_like:
                reason = "technical_sheet_line"
                row.needs_review = False
            elif not price_like:
                reason = "technical_sheet_line"
                row.needs_review = False
            else:
                reason = "duplicate_variant"
            row.exported = False
            add_row_note(row, reason)
            variant_reasons[reason] += 1
            if len(variant_samples) < 3:
                variant_samples.append((item["line_text"] or "")[:160])

        summary_items.append(
            {
                "art_no": art_no,
                "count": len(group_rows),
                "best_score": scored[0]["score"],
                "best_line": (scored[0]["line_text"] or "")[:160],
                "variant_reasons": dict(variant_reasons),
                "variant_samples": variant_samples,
            }
        )

    summary_items.sort(key=lambda item: (-item["count"], item["art_no"]))
    if dedup_info is not None:
        dedup_info["duplicates_summary"] = summary_items[:20]


def select_best_row(rows: List[ProductRow], currency: str) -> ProductRow:
    return max(rows, key=lambda row: row_score(row, currency))


def row_score(row: ProductRow, currency: str, line_info: Optional[Dict[str, object]] = None) -> int:
    if line_info is None:
        line_info = text_utils.analyze_line(row.raw_snippet or row.product_name_raw or "")
    score = 0
    if row.product_name_en:
        score += 3
    if get_price_by_currency(row, currency) is not None or line_info.get("price_like"):
        score += 4
    if line_info.get("has_currency"):
        score += 2
    if row.designer:
        score += 1
    if row.size_raw:
        score += 1
    if row.variant:
        score += 1
    if row.colli is not None:
        score += 1
    if row.section:
        score += 1
    if line_info.get("dimension_line"):
        score -= 3
    if len((row.product_name_raw or "").split()) > 10 and not line_info.get("price_like"):
        score -= 2
    if row.needs_review:
        score -= 1
    if not row.exported:
        score -= 1
    return score


def add_row_note(row: ProductRow, note: str) -> None:
    if not note:
        return
    existing = [item.strip() for item in row.notes.split(";") if item.strip()]
    if note not in existing:
        existing.append(note)
        row.notes = "; ".join(existing)


def log_marker_debug(page_number: int, text: str) -> None:
    rrp_matches = list(re.finditer(r"\*\*\s*RRP", text))
    art_matches = list(re.finditer(r"\bArt\.?\s*no\.?\s*:", text, re.IGNORECASE))
    LOGGER.info(
        "Page %d markers: rrp=%d art_no=%d",
        page_number,
        len(rrp_matches),
        len(art_matches),
    )

    for label, matches in (("RRP", rrp_matches), ("ART", art_matches)):
        for match in matches[:5]:
            start = max(0, match.start() - 30)
            end = min(len(text), match.end() + 30)
            snippet = text[start:end].replace("\n", " ")
            LOGGER.info("%s match: ...%s...", label, snippet)


def get_price_by_currency(row: ProductRow, currency: str) -> Optional[float]:
    currency = currency.upper()
    if currency == "DKK":
        return row.price_dkk
    if currency == "SEK":
        return row.price_sek
    if currency == "NOK":
        return row.price_nok
    return row.price_eur


def apply_currency_filter(row: ProductRow, currency_only: str) -> None:
    if not currency_only:
        return
    currency_only = currency_only.upper()
    if currency_only != "DKK":
        row.price_dkk = None
    if currency_only != "SEK":
        row.price_sek = None
    if currency_only != "NOK":
        row.price_nok = None
    if currency_only != "EUR":
        row.price_eur = None


def print_debug_blocks(blocks: List[dict], count: int) -> None:
    print("\nDEBUG BLOCKS (raw)")
    for idx, block in enumerate(blocks[:count], start=1):
        print("=" * 80)
        print(f"BLOCK {idx} - page {block.get('page')} - section {block.get('section')}")
        print("-" * 80)
        print(block.get("raw_text", ""))
    print("=" * 80)
