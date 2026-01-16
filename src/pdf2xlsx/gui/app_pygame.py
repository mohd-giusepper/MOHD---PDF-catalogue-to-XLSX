import os
import queue
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import pygame
import tkinter as tk
from tkinter import filedialog, messagebox

from pdf2xlsx import config
from pdf2xlsx.core import auto_convert, triage
from pdf2xlsx.io import debug_output
from pdf2xlsx.logging_setup import configure_logging


BG = (245, 245, 245)
CARD = (255, 255, 255)
TEXT = (24, 24, 24)
MUTED = (96, 96, 96)
BORDER = (216, 216, 216)
ACCENT = (18, 18, 18)
ACCENT_HOVER = (32, 32, 32)


@dataclass
class Button:
    rect: pygame.Rect
    label: str
    on_click: Callable[[], None]
    primary: bool = False
    enabled: bool = True

    def draw(self, surface, font, mouse_pos):
        is_hover = self.rect.collidepoint(mouse_pos)
        bg = ACCENT if self.primary else CARD
        fg = (255, 255, 255) if self.primary else TEXT
        if not self.enabled:
            bg = (235, 235, 235)
            fg = (160, 160, 160)
        elif is_hover:
            bg = ACCENT_HOVER if self.primary else (240, 240, 240)
        pygame.draw.rect(surface, bg, self.rect, border_radius=8)
        pygame.draw.rect(surface, BORDER, self.rect, 1, border_radius=8)
        text_surf = font.render(self.label, True, fg)
        text_rect = text_surf.get_rect(center=self.rect.center)
        surface.blit(text_surf, text_rect)

    def handle_event(self, event):
        if not self.enabled:
            return
        if event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
            if self.rect.collidepoint(event.pos):
                self.on_click()


class App:
    def __init__(self) -> None:
        configure_logging("INFO")

        pygame.init()
        pygame.display.set_caption("PDF Catalog Parser")
        self.screen = pygame.display.set_mode((980, 640), pygame.RESIZABLE)
        self.clock = pygame.time.Clock()
        self.font = pygame.font.SysFont("Helvetica Neue", 18) or pygame.font.SysFont(
            "Segoe UI", 18
        )
        self.font_small = pygame.font.SysFont("Helvetica Neue", 14) or pygame.font.SysFont(
            "Segoe UI", 14
        )
        self.font_heading = pygame.font.SysFont("Helvetica Neue", 22, bold=True) or pygame.font.SysFont(
            "Segoe UI", 22, bold=True
        )

        self.scan_results = []
        self.status = "Seleziona file o cartella per iniziare."
        self.progress_total = 0
        self.progress_value = 0
        self._busy_label = ""
        self._stage = ""
        self._triage_scroll = 0

        self.current_folder = Path(config.INPUT_DIR).resolve()
        self.debug_enabled = config.DEBUG_JSON_DEFAULT
        self.selected_file = ""
        self.selected_folder = ""

        self.stop_event = threading.Event()
        self._queue: queue.Queue = queue.Queue()
        self._worker: Optional[threading.Thread] = None

        self._buttons: List[Button] = []
        self._build_buttons()

        # Scan only on explicit user action.

    def _layout(self):
        w, h = self.screen.get_size()
        padding = 20
        top = 70
        left_width = 260
        left_panel = pygame.Rect(padding, top, left_width, h - top - padding)
        right_panel = pygame.Rect(
            left_panel.right + padding,
            top,
            w - left_panel.right - padding * 2,
            h - top - padding,
        )
        return left_panel, right_panel

    def _build_buttons(self):
        self._buttons = []
        left_panel, _ = self._layout()
        x = left_panel.x + 12
        y = left_panel.y + 52
        btn_w = left_panel.width - 16
        btn_h = 38
        gap = 12
        self._buttons.append(
            Button(pygame.Rect(x, y, btn_w, btn_h), "Converti PDF singolo", self._convert_single)
        )
        self._buttons.append(
            Button(
                pygame.Rect(x, y + btn_h + gap, btn_w, btn_h),
                "Converti cartella",
                self._convert_folder,
                primary=True,
            )
        )
        stop_y = left_panel.bottom - btn_h - 16
        self._buttons.append(
            Button(
                pygame.Rect(x, stop_y, btn_w, btn_h),
                "Stop",
                self._request_stop,
            )
        )

    def _request_stop(self) -> None:
        self.stop_event.set()
        self.status = "Stop richiesto..."
        self._busy_label = "Interruzione"

    def _set_status(self, message: str) -> None:
        self.status = message

    def _convert_single(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self.selected_file or self._pick_file()
        if not path:
            return
        self.stop_event.clear()
        self._busy_label = "Conversione singolo"
        self._set_status("Scansione file...")
        self.progress_total = 1
        self.progress_value = 0
        self._worker = threading.Thread(
            target=self._run_single_background, args=(Path(path),), daemon=True
        )
        self._worker.start()

    def _convert_folder(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self.selected_folder or self._pick_folder()
        if not path:
            return
        self.current_folder = Path(path)
        self._start_scan(self.current_folder, convert_ok=True)

    def _start_scan(self, folder: Path, convert_ok: bool) -> None:
        if self._worker and self._worker.is_alive():
            return
        if not folder.exists():
            self._show_error("Cartella input non trovata.")
            return
        self.stop_event.clear()
        self.scan_results = []
        self._triage_scroll = 0
        self.progress_value = 0
        self.progress_total = 0
        self._busy_label = "Scansione cartella"
        self._stage = "scan"
        self._set_status(f"Scanning {folder} ...")
        self._worker = threading.Thread(
            target=self._run_scan_background, args=(folder, convert_ok), daemon=True
        )
        self._worker.start()

    def _run_scan_background(self, folder: Path, convert_ok: bool) -> None:
        try:
            results = triage.scan_folder_recursive(
                str(folder),
                ocr=False,
                progress_callback=self._progress_callback,
                should_stop=self.stop_event.is_set,
            )
        except Exception as exc:
            self._queue.put(("error", str(exc)))
            return

        self._queue.put(("scan_done", results, convert_ok))

        if not convert_ok or self.stop_event.is_set():
            return

        ok_results = [result for result in results if result.decision == "OK"]
        total = len(ok_results)
        for idx, result in enumerate(ok_results, start=1):
            if self.stop_event.is_set():
                break
            self._progress_callback(idx - 1, total, result.source_file, stage="convert")
            auto_convert.run_auto_for_pdf(
                pdf_path=result.source_path,
                output_dir=str(Path(config.OUTPUT_DIR)),
                ocr=False,
                debug_enabled=self.debug_enabled,
                debug_output_dir=str(Path(config.OUTPUT_DIR)),
            )
            self._progress_callback(idx, total, result.source_file, stage="convert")

        for result in results:
            if result.decision == "OK":
                continue
            triage_result, cached_pages = triage.scan_pdf_cached(
                result.source_path, ocr=False
            )
            triage_result.source_file = result.source_file
            triage_result.source_path = result.source_path
            debug_output.write_debug_json(
                result.source_path,
                triage_result,
                cached_pages,
                str(Path(config.OUTPUT_DIR)),
                reason=triage_result.reasons,
                force=True,
            )

        self._queue.put(("convert_done", total))

    def _run_single_background(self, pdf_path: Path) -> None:
        try:
            triage_result, cached_pages = triage.scan_pdf_cached(str(pdf_path), ocr=False)
            triage_result.source_file = pdf_path.name
            triage_result.source_path = str(pdf_path)
            self._queue.put(("scan_done", [triage_result], False))
            force_debug = self.debug_enabled or triage_result.decision in {"FORSE", "NO"}
            result = auto_convert.run_auto_for_pdf(
                pdf_path=str(pdf_path),
                output_dir=str(Path(config.OUTPUT_DIR)),
                ocr=False,
                cached_pages=cached_pages,
                triage_result=triage_result,
                debug_enabled=force_debug,
                debug_output_dir=str(Path(config.OUTPUT_DIR)),
            )
            self._queue.put(("single_done", result.final_status or "COMPLETED"))
        except Exception as exc:
            self._queue.put(("error", str(exc)))

    def _progress_callback(
        self, processed: int, total: int, filename: str, stage: str = "scan"
    ) -> None:
        self._queue.put(("progress", processed, total, filename, stage))

    def _poll_queue(self) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            kind = item[0]
            if kind == "progress":
                _, processed, total, filename, stage = item
                self.progress_total = total
                self.progress_value = processed
                self._stage = stage
                if stage == "convert":
                    self.status = f"Conversione {filename} ({processed}/{total})"
                else:
                    self.status = f"Scansione {filename} ({processed}/{total})"
            elif kind == "scan_done":
                results, convert_ok = item[1], item[2]
                self.scan_results = results
                self.status = "Scansione completata"
                self._busy_label = "Conversione cartella" if convert_ok else ""
            elif kind == "convert_done":
                total = item[1]
                self.status = f"Conversione completata ({total} OK)"
                self._busy_label = ""
            elif kind == "single_done":
                decision = item[1]
                self.status = f"Conversione singolo: {decision}"
                self._busy_label = ""
            elif kind == "error":
                message = item[1]
                self.status = "Errore"
                self._busy_label = ""
                self._show_error(message)

    def _pick_file(self) -> str:
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askopenfilename(
            initialdir=config.INPUT_DIR,
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        root.destroy()
        return path or ""

    def _pick_folder(self) -> str:
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askdirectory(initialdir=config.INPUT_DIR)
        root.destroy()
        return path or ""

    def _select_file(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self._pick_file()
        if path:
            self.selected_file = path

    def _select_folder(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self._pick_folder()
        if path:
            self.selected_folder = path
            self.current_folder = Path(path)
            self.status = "Cartella selezionata. Premi Converti cartella."

    def _show_error(self, message: str) -> None:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Errore", message)
        root.destroy()

    def _draw_panel(self, rect: pygame.Rect, title: str):
        pygame.draw.rect(self.screen, CARD, rect, border_radius=12)
        pygame.draw.rect(self.screen, BORDER, rect, 1, border_radius=12)
        title_surf = self.font_heading.render(title, True, TEXT)
        self.screen.blit(title_surf, (rect.x + 14, rect.y + 12))

    def _draw_text(self, text: str, pos: Tuple[int, int], color=TEXT, font=None):
        font = font or self.font
        surf = font.render(text, True, color)
        self.screen.blit(surf, pos)

    def _truncate(self, text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        if max_chars <= 3:
            return text[:max_chars]
        return text[: max_chars - 3] + "..."

    def run(self) -> None:
        running = True
        while running:
            self._poll_queue()
            self._build_buttons()
            click_pos = None
            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                elif event.type == pygame.VIDEORESIZE:
                    self.screen = pygame.display.set_mode(event.size, pygame.RESIZABLE)
                elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 4:
                    self._triage_scroll = max(0, self._triage_scroll - 1)
                elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 5:
                    self._triage_scroll += 1
                elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                    click_pos = event.pos

            mouse_pos = pygame.mouse.get_pos()
            pending_click = click_pos is not None

            self.screen.fill(BG)

            self._draw_text("PDF Catalog Parser", (20, 20), TEXT, self.font_heading)
            self._draw_text(
                "Converti singolo o cartella (triage + best-of).",
                (20, 46),
                MUTED,
                self.font_small,
            )

            left_panel, right_panel = self._layout()
            self._draw_panel(left_panel, "Azioni")
            self._draw_panel(right_panel, "Scan input")

            is_busy = self._worker is not None and self._worker.is_alive()
            header_height = 48

            for button in self._buttons:
                if button.label != "Stop":
                    button.enabled = not is_busy
                if button.label == "Stop":
                    button.enabled = True
                button.draw(self.screen, self.font, mouse_pos)
                if pending_click and button.enabled and button.rect.collidepoint(click_pos):
                    button.on_click()

            # Browse section
            browse_y = left_panel.y + 148
            self._draw_text("Sfoglia", (left_panel.x + 16, browse_y), MUTED, self.font_small)
            browse_file = Button(
                pygame.Rect(left_panel.x + 12, browse_y + 22, left_panel.width - 24, 30),
                "Seleziona file",
                self._select_file,
            )
            browse_folder = Button(
                pygame.Rect(left_panel.x + 12, browse_y + 58, left_panel.width - 24, 30),
                "Seleziona cartella",
                self._select_folder,
            )
            for temp_btn in (browse_file, browse_folder):
                temp_btn.enabled = not is_busy
                temp_btn.draw(self.screen, self.font_small, mouse_pos)
                if pending_click and temp_btn.enabled and temp_btn.rect.collidepoint(click_pos):
                    temp_btn.on_click()

            file_path_text = self._truncate(self.selected_file or "(nessun file)", 28)
            folder_path_text = self._truncate(self.selected_folder or "(nessuna cartella)", 28)
            self._draw_text(
                f"File: {file_path_text}",
                (left_panel.x + 16, browse_y + 96),
                MUTED,
                self.font_small,
            )
            self._draw_text(
                f"Cartella: {folder_path_text}",
                (left_panel.x + 16, browse_y + 116),
                MUTED,
                self.font_small,
            )

            # Debug toggle
            debug_rect = pygame.Rect(left_panel.x + 16, browse_y + 144, 18, 18)
            pygame.draw.rect(self.screen, CARD, debug_rect, border_radius=3)
            pygame.draw.rect(self.screen, BORDER, debug_rect, 1, border_radius=3)
            if self.debug_enabled:
                pygame.draw.line(
                    self.screen,
                    ACCENT,
                    (debug_rect.x + 3, debug_rect.centery),
                    (debug_rect.centerx, debug_rect.bottom - 3),
                    2,
                )
                pygame.draw.line(
                    self.screen,
                    ACCENT,
                    (debug_rect.centerx, debug_rect.bottom - 3),
                    (debug_rect.right - 3, debug_rect.y + 3),
                    2,
                )
            self._draw_text("Debug", (debug_rect.right + 8, debug_rect.y - 1), TEXT, self.font_small)
            if pending_click and debug_rect.collidepoint(click_pos):
                self.debug_enabled = not self.debug_enabled

            # Table
            table_rect = pygame.Rect(
                right_panel.x + 16,
                right_panel.y + header_height,
                right_panel.width - 32,
                right_panel.height - header_height - 88,
            )
            pygame.draw.rect(self.screen, CARD, table_rect, border_radius=8)
            pygame.draw.rect(self.screen, BORDER, table_rect, 1, border_radius=8)

            headers = ["File", "Decisione", "Parser", "Nota"]
            table_inner_width = table_rect.width - 16
            col_widths = [
                int(table_inner_width * 0.46),
                int(table_inner_width * 0.14),
                int(table_inner_width * 0.16),
            ]
            col_widths.append(table_inner_width - sum(col_widths))
            x = table_rect.x + 8
            y = table_rect.y + 8
            for header, width in zip(headers, col_widths):
                self._draw_text(header, (x, y), MUTED, self.font_small)
                x += width

            row_y = y + 26
            row_height = 28
            visible_rows = max(4, int((table_rect.height - 40) / row_height))
            start_idx = min(self._triage_scroll, max(0, len(self.scan_results) - visible_rows))
            for result in self.scan_results[start_idx:start_idx + visible_rows]:
                row_color = CARD
                if result.decision == "OK":
                    row_color = (236, 244, 236)
                elif result.decision == "FORSE":
                    row_color = (247, 241, 232)
                elif result.decision == "NO":
                    row_color = (248, 236, 236)
                pygame.draw.rect(
                    self.screen,
                    row_color,
                    pygame.Rect(table_rect.x + 6, row_y - 2, table_rect.width - 12, 26),
                    border_radius=6,
                )
                x = table_rect.x + 8
                cells = [
                    result.source_file or "",
                    result.decision or "",
                    result.suggested_profile or "",
                    result.reasons or "",
                ]
                for cell, width in zip(cells, col_widths):
                    max_chars = max(8, int(width / 7))
                    self._draw_text(
                        self._truncate(str(cell), max_chars),
                        (x, row_y),
                        TEXT,
                        self.font_small,
                    )
                    x += width
                row_y += row_height

            # Progress + status (bottom)
            footer_rect = pygame.Rect(20, self.screen.get_height() - 52, self.screen.get_width() - 40, 36)
            pygame.draw.rect(self.screen, CARD, footer_rect, border_radius=10)
            pygame.draw.rect(self.screen, BORDER, footer_rect, 1, border_radius=10)
            self._draw_text(
                self.status,
                (footer_rect.x + 12, footer_rect.y + 9),
                MUTED,
                self.font_small,
            )
            bar_rect = pygame.Rect(
                footer_rect.right - 220,
                footer_rect.y + 10,
                200,
                12,
            )
            pygame.draw.rect(self.screen, (230, 230, 230), bar_rect, border_radius=8)
            if self.progress_total:
                ratio = min(1.0, self.progress_value / max(1, self.progress_total))
                fill_rect = pygame.Rect(
                    bar_rect.x, bar_rect.y, int(bar_rect.width * ratio), bar_rect.height
                )
                pygame.draw.rect(self.screen, (46, 160, 67), fill_rect, border_radius=8)
            if self.progress_total:
                progress_label = f"{self.progress_value}/{self.progress_total}"
                self._draw_text(
                    progress_label,
                    (bar_rect.x - 40, bar_rect.y - 2),
                    MUTED,
                    self.font_small,
                )

            pygame.display.flip()
            self.clock.tick(30)

        pygame.quit()


def start() -> None:
    App().run()
