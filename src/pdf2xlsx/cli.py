import argparse
from pathlib import Path

from pdf2xlsx import config
from pdf2xlsx.core import pipeline
from pdf2xlsx.logging_setup import configure_logging
from pdf2xlsx.utils.pages import parse_pages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a Stelton PDF pricelist into an XLSX table."
    )
    parser.add_argument(
        "--input",
        default="",
        help="Path to input PDF or folder (default: ./input).",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Path to output XLSX or folder (default: ./output).",
    )
    parser.add_argument(
        "--pages",
        default="",
        help='Pages to parse, e.g. "2-5" or "2,3,4". Defaults to all.',
    )
    parser.add_argument(
        "--debug-json",
        default="",
        help="Optional path to save debug JSON with raw blocks and parsed rows.",
    )
    parser.add_argument(
        "--debug-blocks",
        type=int,
        default=0,
        help="Print N raw blocks to console for inspection.",
    )
    parser.add_argument(
        "--debug-matches",
        action="store_true",
        help="Print marker counts and sample contexts on pages 2-3.",
    )
    parser.add_argument(
        "--currency-only",
        default="",
        choices=["", "DKK", "SEK", "NOK", "EUR"],
        help="If set, populate only the chosen currency column.",
    )
    parser.add_argument(
        "--parser",
        default=config.DEFAULT_PARSER,
        help="Parser name to use (default: stelton_2025).",
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="Use OCR when text extraction returns empty.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail with non-zero exit code on OCR use, duplicate conflicts, or high review rate.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    try:
        input_path = resolve_input_path(args.input)
        output_path = resolve_output_path(args.output, input_path)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1

    report = pipeline.run_pipeline(
        input_pdf=input_path,
        output_xlsx=output_path,
        pages=parse_pages(args.pages or ""),
        debug_json=args.debug_json or None,
        parser_name=args.parser or config.DEFAULT_PARSER,
        debug_blocks=args.debug_blocks,
        currency_only=args.currency_only or None,
        ocr=args.ocr,
        debug_matches=args.debug_matches,
    )
    print_run_summary(report)
    return evaluate_strict_exit(report, args.strict)


def resolve_input_path(input_arg: str) -> str:
    if input_arg:
        path = Path(input_arg)
        if path.is_dir():
            pdfs = sorted(path.glob("*.pdf"))
            if len(pdfs) == 1:
                return str(pdfs[0])
            if len(pdfs) == 0:
                raise ValueError(f"No PDFs found in {path}")
            raise ValueError(f"Multiple PDFs found in {path}; specify --input file.")
        if not path.exists():
            raise ValueError(f"Input file not found: {path}")
        return str(path)

    default_dir = Path(config.INPUT_DIR)
    if not default_dir.exists():
        raise ValueError(f"Default input folder not found: {default_dir}")
    pdfs = sorted(default_dir.glob("*.pdf"))
    if len(pdfs) == 1:
        return str(pdfs[0])
    if len(pdfs) == 0:
        raise ValueError(f"No PDFs found in {default_dir}")
    raise ValueError(
        f"Multiple PDFs found in {default_dir}; specify --input file."
    )


def resolve_output_path(output_arg: str, input_path: str) -> str:
    input_stem = Path(input_path).stem
    output_dir = Path(config.OUTPUT_DIR)

    if output_arg:
        out_path = Path(output_arg)
        if out_path.suffix.lower() == ".xlsx":
            target = out_path
        else:
            target = out_path / f"{input_stem}.xlsx"
    else:
        target = output_dir / f"{input_stem}.xlsx"

    target.parent.mkdir(parents=True, exist_ok=True)
    return str(target)


def print_run_summary(report) -> None:
    rows_total = len(report.rows)
    needs_review_pct = (report.rows_needs_review / rows_total * 100) if rows_total else 0.0
    currency_label = report.target_currency.lower()

    print("\nSUMMARY")
    print(f"pages_processed: {report.pages_processed}")
    print(f"pages_needing_ocr: {report.pages_needing_ocr}")
    print(f"pages_ocr_used: {report.pages_ocr_used}")
    print(f"rows_total: {rows_total}")
    print(f"rows_exported: {report.rows_exported}")
    print(f"rows_needs_review: {report.rows_needs_review} ({needs_review_pct:.1f}%)")
    print(f"review_rows: {report.rows_needs_review}")
    print(f"missing_art_no: {report.missing_art_no}")
    print(f"missing_price_{currency_label}: {report.missing_price}")
    print(f"duplicate_art_no_count: {report.duplicate_art_no_count}")
    print(f"duplicate_conflicts_count: {report.duplicate_conflicts_count}")
    if report.duplicate_art_no_top:
        print("duplicate_art_no_top:")
        for art_no, count in report.duplicate_art_no_top:
            print(f"- {art_no}: {count}")
    if report.duplicate_conflicts:
        conflicts = ", ".join(report.duplicate_conflicts[:10])
        print(f"duplicate_conflicts: {conflicts}")

    if report.config_info:
        print("\nCONFIG")
        for key, value in report.config_info.items():
            print(f"{key}: {value}")

    if report.review_reasons_top:
        print("\nREVIEW REASONS (top5)")
        for reason, count in report.review_reasons_top:
            print(f"- {reason}: {count}")

    print("\nEXAMPLES (complete)")
    if report.examples_ok:
        for row in report.examples_ok:
            print(format_row_example(row, report.target_currency))
    else:
        print("- none")

    print("\nEXAMPLES (needs_review)")
    if report.examples_needs_review:
        for row in report.examples_needs_review:
            print(format_row_example(row, report.target_currency, include_notes=True))
    else:
        print("- none")


def format_row_example(row, currency: str, include_notes: bool = False) -> str:
    currency = currency.upper()
    price_value = None
    if currency == "DKK":
        price_value = row.price_dkk
    elif currency == "SEK":
        price_value = row.price_sek
    elif currency == "NOK":
        price_value = row.price_nok
    else:
        price_value = row.price_eur
    parts = [
        f"art_no={row.art_no or '-'}",
        f"name={row.product_name_en or '-'}",
        f"price_{currency.lower()}={price_value if price_value is not None else '-'}",
        f"page={row.page or '-'}",
    ]
    if include_notes:
        parts.append(f"notes={row.notes or '-'}")
    return "- " + " | ".join(parts)


def evaluate_strict_exit(report, strict: bool) -> int:
    if not strict:
        if strict_conditions_failed(report):
            print("WARNING: strict conditions would fail. See summary.")
        return 0
    if strict_conditions_failed(report):
        print("STRICT FAIL: One or more strict conditions failed.")
        return 2
    return 0


def strict_conditions_failed(report) -> bool:
    rows_total = len(report.rows)
    review_rate = (report.rows_needs_review / rows_total) if rows_total else 0.0
    if report.pages_ocr_used > 0:
        return True
    if report.duplicate_conflicts_count > 0:
        return True
    if report.config_info:
        threshold = report.config_info.get("review_rate_threshold", 0.15)
    else:
        threshold = 0.15
    return review_rate > threshold


if __name__ == "__main__":
    raise SystemExit(main())
