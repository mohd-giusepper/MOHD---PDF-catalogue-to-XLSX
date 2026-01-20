import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from pdf2xlsx import config
from pdf2xlsx.core import auto_convert, page_cache


class SamplingTest(unittest.TestCase):
    def test_sample_count_buckets(self) -> None:
        self.assertEqual(page_cache.compute_sample_count(40), 12)
        self.assertEqual(page_cache.compute_sample_count(150), 20)
        self.assertEqual(page_cache.compute_sample_count(350), 30)
        self.assertEqual(page_cache.compute_sample_count(900), 40)

    def test_retry_on_too_few_rows_logs(self) -> None:
        dummy_page = page_cache.CachedPage(
            page_number=1,
            text="",
            normalized_text="",
            lines=[],
            words=[],
            text_len=0,
            images_count=0,
            needs_ocr=False,
            ocr_used=False,
            table_hint=False,
        )
        cached_pages = [dummy_page]
        meta_first = {"sample_count": 10, "pages_sampled": [1], "num_pages": 10}
        meta_second = {"sample_count": 20, "pages_sampled": [1, 2], "num_pages": 10}

        original_sweep = config.CACHE_ENABLE_SWEEP_AFTER_RETRY
        config.CACHE_ENABLE_SWEEP_AFTER_RETRY = False
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                with mock.patch(
                    "pdf2xlsx.core.page_cache.build_signal_cache",
                    side_effect=[(cached_pages, [], meta_first), (cached_pages, [], meta_second)],
                ):
                    with mock.patch(
                        "pdf2xlsx.core.auto_convert.evaluate_parser_fast",
                        return_value={
                            "parser": "table_based",
                            "ok": False,
                            "reason": "too_few_rows",
                            "metrics": {"eval_score": 0.0},
                        },
                    ):
                        with self.assertLogs(
                            "pdf2xlsx.core.auto_convert", level="INFO"
                        ) as logs:
                            auto_convert.run_auto_for_pdf(
                                pdf_path="dummy.pdf",
                                output_dir=temp_dir,
                                fast_eval_only=True,
                            )
            self.assertTrue(
                any("RETRY_SCAN" in message for message in logs.output),
                "Missing RETRY_SCAN log entry",
            )
        finally:
            config.CACHE_ENABLE_SWEEP_AFTER_RETRY = original_sweep

    def test_diagnostic_xlsx_written_on_failure(self) -> None:
        dummy_page = page_cache.CachedPage(
            page_number=1,
            text="",
            normalized_text="",
            lines=[],
            words=[],
            text_len=0,
            images_count=0,
            needs_ocr=False,
            ocr_used=False,
            table_hint=False,
        )
        cached_pages = [dummy_page]
        meta = {
            "sample_count": 10,
            "pages_sampled": [1],
            "num_pages": 10,
            "top_k_pages": [1],
            "top_k_scores": [1.0],
        }

        original_sweep = config.CACHE_ENABLE_SWEEP_AFTER_RETRY
        config.CACHE_ENABLE_SWEEP_AFTER_RETRY = False
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                with mock.patch(
                    "pdf2xlsx.core.page_cache.build_signal_cache",
                    return_value=(cached_pages, [], meta),
                ):
                    with mock.patch(
                        "pdf2xlsx.core.auto_convert.evaluate_parser_fast",
                        return_value={
                            "parser": "table_based",
                            "ok": False,
                            "reason": "too_few_rows",
                            "metrics": {"eval_score": 0.0},
                        },
                    ):
                        auto_convert.run_auto_for_pdf(
                            pdf_path="dummy.pdf",
                            output_dir=temp_dir,
                            fast_eval_only=True,
                        )
                diagnostic_path = os.path.join(temp_dir, "dummy.diagnostic.xlsx")
                self.assertTrue(os.path.exists(diagnostic_path))
        finally:
            config.CACHE_ENABLE_SWEEP_AFTER_RETRY = original_sweep


if __name__ == "__main__":
    unittest.main()
