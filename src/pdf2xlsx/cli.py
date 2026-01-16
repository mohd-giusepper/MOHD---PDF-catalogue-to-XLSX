import argparse
from pathlib import Path

from pdf2xlsx import config
from pdf2xlsx.core import auto_convert, triage
from pdf2xlsx.io.run_debug import RunDebugCollector
from pdf2xlsx.logging_setup import configure_logging


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PDF to XLSX converter.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="Path to a single PDF.")
    group.add_argument("--folder", help="Path to a folder containing PDFs.")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Write <pdf>.debug.json for audit (also written on failures).",
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

    output_dir = Path(config.OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    debug_dir = Path(config.DEBUG_PACK_DIR)
    debug_dir.mkdir(parents=True, exist_ok=True)
    run_debug = RunDebugCollector.start(
        input_root=str(args.file or args.folder or ""), run_type="cli"
    )

    if args.file:
        return run_single(Path(args.file), output_dir, debug_dir, args.debug, run_debug)
    return run_folder(Path(args.folder), output_dir, debug_dir, args.debug, run_debug)


def run_single(
    pdf_path: Path,
    output_dir: Path,
    debug_dir: Path,
    debug_enabled: bool,
    run_debug: RunDebugCollector,
) -> int:
    if not pdf_path.exists():
        print(f"ERROR: file not found: {pdf_path}")
        return 1

    triage_result, cached_pages = triage.scan_pdf_cached(str(pdf_path), ocr=False)
    result = auto_convert.run_auto_for_pdf(
        pdf_path=str(pdf_path),
        output_dir=str(output_dir),
        ocr=False,
        cached_pages=cached_pages,
        triage_result=triage_result,
        run_debug=run_debug,
    )
    run_debug.write(str(debug_dir))
    if (result.final_status or "").startswith("FAILED"):
        return 1
    return 0


def run_folder(
    folder_path: Path,
    output_dir: Path,
    debug_dir: Path,
    debug_enabled: bool,
    run_debug: RunDebugCollector,
) -> int:
    if not folder_path.exists():
        print(f"ERROR: folder not found: {folder_path}")
        return 1

    results = triage.scan_folder_recursive(str(folder_path), ocr=False)
    for idx, result in enumerate(results):
        if result.decision not in {"OK", "FORSE"}:
            continue
        converted = auto_convert.run_auto_for_pdf(
            pdf_path=result.source_path,
            output_dir=str(output_dir),
            ocr=False,
            run_debug=run_debug,
        )
        converted.source_file = result.source_file
        converted.source_path = result.source_path
        results[idx] = converted

    for result in results:
        if result.decision in {"OK", "FORSE"}:
            continue
        triage_result, cached_pages = triage.scan_pdf_cached(
            result.source_path, ocr=False
        )
        triage_result.source_file = result.source_file
        triage_result.source_path = result.source_path
        run_debug.add_pdf(
            result.source_path,
            triage_result,
            cached_pages,
            reason=triage_result.reasons,
        )

    run_debug.write(str(debug_dir))
    return 0
