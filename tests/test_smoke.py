import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pdf2xlsx.core import pipeline


class SmokeTest(unittest.TestCase):
    def test_smoke(self) -> None:
        default_pdf = ROOT / "1 EURO Stelton pricelist  2025-1.pdf"
        pdf_path = Path(os.environ.get("PDF_PATH", str(default_pdf)))
        if not pdf_path.exists():
            self.skipTest(f"PDF not found: {pdf_path}")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
            output_path = temp_file.name

        try:
            report = pipeline.run_pipeline(
                input_pdf=str(pdf_path),
                output_xlsx=output_path,
                pages=[2, 3, 4, 5],
                debug_json=None,
                parser_name="stelton_2025",
            )
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

        rows = report.rows
        self.assertGreater(len(rows), 0)
        self.assertEqual(report.pages_ocr_used, 0)
        self.assertEqual(report.missing_art_no, 0)
        price_eur_count = sum(1 for row in rows if row.price_eur is not None)
        self.assertGreater(price_eur_count, 10)
        self.assertGreaterEqual(price_eur_count / len(rows), 0.3)


if __name__ == "__main__":
    unittest.main()
