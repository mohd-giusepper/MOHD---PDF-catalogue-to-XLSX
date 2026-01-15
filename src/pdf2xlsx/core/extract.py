import logging
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

import pdfplumber

from pdf2xlsx import config


LOGGER = logging.getLogger(__name__)


@dataclass
class PageData:
    page_number: int
    text: str
    words: List[dict]
    text_len: int
    images_count: int
    needs_ocr: bool
    ocr_used: bool


def extract_pages(
    pdf_path: str,
    pages: Optional[List[int]] = None,
    ocr: bool = False,
    progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
) -> Iterable[PageData]:
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        page_numbers = pages or list(range(1, total_pages + 1))
        total_selected = len(page_numbers)
        processed = 0

        for page_number in page_numbers:
            if page_number < 1 or page_number > total_pages:
                LOGGER.warning("Page out of range: %d", page_number)
                continue
            page = pdf.pages[page_number - 1]
            text = page.extract_text() or ""
            words = page.extract_words() or []
            images_count = len(getattr(page, "images", []))
            text_len = len(text.strip())
            needs_ocr = page_needs_ocr(text_len, images_count)
            ocr_used = False

            if ocr and needs_ocr:
                ocr_text = ocr_page(pdf_path, page_number)
                if ocr_text:
                    text = ocr_text
                    ocr_used = True
                    words = []

            processed += 1
            if progress_callback:
                progress_callback(processed, total_selected, page_number, total_pages)

            yield PageData(
                page_number=page_number,
                text=text,
                words=words,
                text_len=text_len,
                images_count=images_count,
                needs_ocr=needs_ocr,
                ocr_used=ocr_used,
            )


def ocr_page(pdf_path: str, page_number: int) -> str:
    try:
        from pdf2image import convert_from_path
        import pytesseract
    except ImportError as exc:
        raise RuntimeError(
            "OCR requested but pdf2image/pytesseract are not installed."
        ) from exc

    images = convert_from_path(
        pdf_path, first_page=page_number, last_page=page_number
    )
    if not images:
        return ""
    return pytesseract.image_to_string(images[0], lang="eng")


def page_needs_ocr(text_len: int, images_count: int) -> bool:
    return text_len < config.THRESHOLD_TEXT_LEN_FOR_OCR and images_count > 0
