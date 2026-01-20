import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pdf2xlsx.core import table_stitcher


class TableStitcherTests(unittest.TestCase):
    def test_stitcher_pairs_code_and_price_columns(self) -> None:
        words = [
            {"text": "AB1234", "x0": 10.0, "top": 100.0},
            {"text": "Lamp", "x0": 60.0, "top": 100.0},
            {"text": "EUR", "x0": 220.0, "top": 100.0},
            {"text": "120", "x0": 250.0, "top": 100.0},
        ]
        rows, meta = table_stitcher.stitch_page_words(
            words=words,
            target_currency="EUR",
            max_rows=10,
            y_tolerance=2.0,
        )
        self.assertTrue(rows)
        self.assertGreater(meta.get("rows_built", 0), 0)
        self.assertIn("AB1234", rows[0].get("line_text", ""))
        self.assertIn("120", rows[0].get("line_text", ""))


if __name__ == "__main__":
    unittest.main()
