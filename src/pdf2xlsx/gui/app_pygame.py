import os
import ctypes
import queue
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import pygame
import tkinter as tk
from tkinter import filedialog, messagebox

from pdf2xlsx import config
from pdf2xlsx.core import auto_convert, page_cache, triage
from pdf2xlsx.io.run_debug import RunDebugCollector
from pdf2xlsx.logging_setup import configure_logging
from pdf2xlsx.models import TriageResult


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
        text_surf = font.render(self.label, False, fg)
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

        os.environ.setdefault("SDL_RENDER_SCALE_QUALITY", "0")
        os.environ.setdefault("SDL_VIDEO_HIGHDPI_DISABLED", "1")
        try:
            ctypes.WinDLL("user32").SetProcessDPIAware()
        except Exception:
            pass
        pygame.init()
        pygame.display.set_caption("PDF Catalog Parser")
        self.screen = pygame.display.set_mode((1120, 720), pygame.RESIZABLE)
        self.clock = pygame.time.Clock()
        font_name = self._pick_font_name(
            ["Helvetica Neue", "Segoe UI", "Arial", "Helvetica", "Verdana"]
        )
        if font_name:
            self.font = pygame.font.SysFont(font_name, 20)
            self.font_small = pygame.font.SysFont(font_name, 16)
            self.font_heading = pygame.font.SysFont(font_name, 26, bold=True)
        else:
            self.font = pygame.font.Font(None, 20)
            self.font_small = pygame.font.Font(None, 16)
            self.font_heading = pygame.font.Font(None, 26)

        self.scan_results: List[TriageResult] = []
        self._cached_pages_map: dict = {}
        self.status = "Seleziona file o cartella per iniziare."
        self.progress_total = 0
        self.progress_value = 0
        self._progress_ratio_target = 0.0
        self._progress_ratio_display = 0.0
        self._busy_label = ""
        self._stage = ""
        self._eta_stage = ""
        self._eta_start = None
        self._eta_seconds = None
        self._triage_scroll = 0

        self.current_folder = Path(config.INPUT_DIR).resolve()
        self.debug_enabled = config.DEBUG_JSON_DEFAULT
        self._run_debug: Optional[RunDebugCollector] = None

        self.stop_event = threading.Event()
        self._queue: queue.Queue = queue.Queue()
        self._worker: Optional[threading.Thread] = None

        self._buttons: List[Button] = []
        self._build_buttons()

        # Scan only on explicit user action.

    def _pick_font_name(self, candidates: List[str]) -> Optional[str]:
        for name in candidates:
            if pygame.font.match_font(name):
                return name
        return None

    def _layout(self):
        w, h = self.screen.get_size()
        padding = 20
        top = 70
        footer_height = 64
        left_width = 260
        left_panel = pygame.Rect(
            padding, top, left_width, h - top - padding - footer_height
        )
        right_panel = pygame.Rect(
            left_panel.right + padding,
            top,
            w - left_panel.right - padding * 2,
            h - top - padding - footer_height,
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
            Button(pygame.Rect(x, y, btn_w, btn_h), "Carica PDF singolo", self._load_single)
        )
        self._buttons.append(
            Button(
                pygame.Rect(x, y + btn_h + gap, btn_w, btn_h),
                "Carica cartella input",
                self._load_folder,
            )
        )
        self._buttons.append(
            Button(
                pygame.Rect(x, y + (btn_h + gap) * 2, btn_w, btn_h),
                "Converti",
                self._convert_loaded,
                primary=True,
            )
        )
        cache_h = 26
        cache_w = int(btn_w * 0.6)
        cache_y = left_panel.bottom - btn_h - cache_h - 22
        self._buttons.append(
            Button(
                pygame.Rect(x, cache_y, cache_w, cache_h),
                "Cache",
                self._clear_cache,
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

    def _clear_cache(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        removed = page_cache.clear_cache_dir()
        self.status = f"Cache pulita ({removed} file)."
        root = tk.Tk()
        root.withdraw()
        messagebox.showinfo("Cache", f"Cache pulita ({removed} file).")
        root.destroy()

    def _set_status(self, message: str) -> None:
        self.status = message

    def _load_single(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self._pick_file()
        if not path:
            return
        pdf_path = Path(path)
        self.current_folder = pdf_path.parent
        targets = [(pdf_path, pdf_path.name)]
        self._start_scan(targets)

    def _load_folder(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self._pick_folder()
        if not path:
            return
        folder = Path(path)
        if not folder.exists():
            self._show_error("Cartella input non trovata.")
            return
        pdfs = sorted(folder.rglob("*.pdf"))
        if not pdfs:
            self._show_error("Nessun PDF trovato nella cartella.")
            return
        self.current_folder = folder
        targets = [(pdf_path, str(pdf_path.relative_to(folder))) for pdf_path in pdfs]
        self._start_scan(targets)

    def _convert_loaded(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        convert_targets = [
            (idx, result)
            for idx, result in enumerate(self.scan_results)
            if result.decision in {"OK", "FORSE"}
        ]
        if not convert_targets:
            self.status = "Nessun file convertibile."
            return
        self.stop_event.clear()
        self._run_debug = RunDebugCollector.start(
            input_root=str(self.current_folder), run_type="convert"
        )
        self.progress_value = 0
        self.progress_total = len(convert_targets)
        self._progress_ratio_target = 0.0
        self._progress_ratio_display = 0.0
        self._busy_label = "Conversione"
        self._stage = "convert"
        self._reset_eta("convert")
        self._set_status(f"Conversione {len(convert_targets)} file...")
        self._worker = threading.Thread(
            target=self._run_convert_background, args=(convert_targets,), daemon=True
        )
        self._worker.start()

    def _start_scan(self, targets: List[Tuple[Path, str]]) -> None:
        if self._worker and self._worker.is_alive():
            return
        if not targets:
            self._show_error("Nessun PDF selezionato.")
            return
        self.stop_event.clear()
        self._run_debug = RunDebugCollector.start(
            input_root=str(self.current_folder), run_type="scan"
        )
        self.scan_results = []
        self._cached_pages_map = {}
        self._triage_scroll = 0
        self.progress_value = 0
        self.progress_total = len(targets)
        self._progress_ratio_target = 0.0
        self._progress_ratio_display = 0.0
        self._busy_label = "Analisi file"
        self._stage = "scan"
        self._reset_eta("scan")
        self._set_status(f"Caricati {len(targets)} file. Avvio analisi...")
        for idx, (path, display_name) in enumerate(targets):
            placeholder = TriageResult(
                source_file=display_name,
                source_path=str(path),
            )
            self.scan_results.append(placeholder)
        self._worker = threading.Thread(
            target=self._run_scan_files_background, args=(targets,), daemon=True
        )
        self._worker.start()

    def _run_scan_files_background(self, targets: List[Tuple[Path, str]]) -> None:
        results: List[TriageResult] = []
        total = len(targets)
        for idx, (pdf_path, display_name) in enumerate(targets, start=1):
            if self.stop_event.is_set():
                break
            self._progress_callback(idx - 1, total, display_name, stage="scan")
            cached_pages = []
            try:
                triage_result, cached_pages = triage.scan_pdf_cached(
                    str(pdf_path), ocr=False
                )
                triage_result.source_file = display_name
                triage_result.source_path = str(pdf_path)
                self._cached_pages_map[str(pdf_path)] = cached_pages
            except Exception as exc:
                triage_result = TriageResult(
                    source_file=display_name,
                    source_path=str(pdf_path),
                    decision="NO",
                    suggested_profile="error",
                    reasons=f"triage_error:{exc}",
                )
            results.append(triage_result)
            self._queue.put(("scan_result", idx - 1, triage_result))
            self._progress_callback(idx, total, display_name, stage="scan")
            if self._run_debug:
                self._run_debug.add_pdf(
                    str(pdf_path),
                    triage_result,
                    cached_pages,
                    reason=triage_result.reasons,
                )
        self._queue.put(("scan_done", results, self.stop_event.is_set()))

    def _run_convert_background(
        self, targets: List[Tuple[int, TriageResult]]
    ) -> None:
        total = len(targets)
        converted = 0
        for idx, (row_index, result) in enumerate(targets, start=1):
            if self.stop_event.is_set():
                break

            def page_progress(
                processed: int,
                total_selected: int,
                page_number: int,
                total_pages: int,
            ) -> None:
                self._queue.put(
                    (
                        "page_progress",
                        processed,
                        total_selected,
                        page_number,
                        total_pages,
                        idx,
                        total,
                        result.source_file,
                    )
                )

            converted_result = auto_convert.run_auto_for_pdf(
                pdf_path=result.source_path,
                output_dir=str(Path(config.OUTPUT_DIR)),
                ocr=False,
                cached_pages=self._cached_pages_map.get(result.source_path),
                run_debug=self._run_debug,
                progress_callback=page_progress,
            )
            converted_result.source_file = result.source_file
            converted_result.source_path = result.source_path
            self._queue.put(("convert_result", row_index, converted_result))
            converted += 1
            self._queue.put(
                ("page_progress", 1, 1, 1, 1, idx, total, result.source_file)
            )
        self._queue.put(("convert_done", converted, self.stop_event.is_set()))

    def _progress_callback(
        self, processed: int, total: int, filename: str, stage: str = "scan"
    ) -> None:
        self._queue.put(("progress", processed, total, filename, stage))

    def _reset_eta(self, stage: str) -> None:
        self._eta_stage = stage
        self._eta_start = None
        self._eta_seconds = None

    def _update_eta(self, processed: int, total: int, stage: str) -> None:
        if total <= 0 or processed <= 0:
            return
        now = time.monotonic()
        if self._eta_stage != stage or self._eta_start is None:
            self._eta_stage = stage
            self._eta_start = now
            self._eta_seconds = None
        elapsed = max(0.0, now - self._eta_start)
        avg = elapsed / max(1, processed)
        remaining = max(0.0, (total - processed) * avg)
        if self._eta_seconds is None:
            self._eta_seconds = remaining
        else:
            self._eta_seconds = self._eta_seconds * 0.7 + remaining * 0.3

    def _format_eta(self) -> str:
        if self._eta_seconds is None:
            return "--:--"
        seconds = int(round(self._eta_seconds))
        if seconds < 0:
            return "--:--"
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{sec:02d}"
        return f"{minutes:02d}:{sec:02d}"

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
                if total:
                    self._progress_ratio_target = processed / max(1, total)
                else:
                    self._progress_ratio_target = 0.0
                if processed == 0:
                    self._reset_eta(stage)
                self._update_eta(processed, total, stage)
                if stage == "convert":
                    self.status = f"Conversione {filename} ({processed}/{total})"
                else:
                    self.status = f"Analisi {filename} ({processed}/{total})"
            elif kind == "page_progress":
                (
                    _,
                    processed,
                    total_selected,
                    page_number,
                    total_pages,
                    file_idx,
                    file_total,
                    filename,
                ) = item
                self.progress_total = total_selected
                self.progress_value = processed
                self._stage = "convert"
                if total_selected:
                    self._progress_ratio_target = processed / max(1, total_selected)
                else:
                    self._progress_ratio_target = 0.0
                if processed == 0:
                    self._reset_eta("convert_page")
                self._update_eta(processed, total_selected, "convert_page")
                self.status = (
                    f"Conversione {filename} pagina {processed}/{total_selected} "
                    f"(file {file_idx}/{file_total})"
                )
            elif kind == "scan_result":
                row_index, result = item[1], item[2]
                if 0 <= row_index < len(self.scan_results):
                    self.scan_results[row_index] = result
            elif kind == "scan_done":
                results, partial = item[1], item[2]
                if len(results) == len(self.scan_results):
                    self.scan_results = results
                self._busy_label = ""
                self._stage = ""
                self._reset_eta("")
                self.status = "Analisi interrotta." if partial else "Analisi completata."
                self._write_summary_async()
            elif kind == "convert_result":
                row_index, result = item[1], item[2]
                if 0 <= row_index < len(self.scan_results):
                    self.scan_results[row_index] = result
            elif kind == "convert_done":
                total, partial = item[1], item[2]
                self.status = (
                    f"Conversione interrotta ({total} completati)"
                    if partial
                    else f"Conversione completata ({total} completati)"
                )
                self._busy_label = ""
                self._stage = ""
                self._reset_eta("")
                self._write_summary_async()
            elif kind == "error":
                message = item[1]
                self.status = "Errore"
                self._busy_label = ""
                self._show_error(message)

    def _pick_file(self) -> str:
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askopenfilename(
            initialdir=str(self.current_folder),
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        root.destroy()
        return path or ""

    def _pick_folder(self) -> str:
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askdirectory(initialdir=str(self.current_folder))
        root.destroy()
        return path or ""

    def _show_error(self, message: str) -> None:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Errore", message)
        root.destroy()

    def _write_summary_async(self) -> None:
        if not self._run_debug:
            return
        debug_dir = Path(config.DEBUG_PACK_DIR)
        debug_dir.mkdir(parents=True, exist_ok=True)
        thread = threading.Thread(
            target=self._run_debug.write,
            args=(str(debug_dir),),
            daemon=True,
        )
        thread.start()
        self._run_debug = None

    def _draw_panel(self, rect: pygame.Rect, title: str):
        pygame.draw.rect(self.screen, CARD, rect, border_radius=12)
        pygame.draw.rect(self.screen, BORDER, rect, 1, border_radius=12)
        title_surf = self.font_heading.render(title, False, TEXT)
        self.screen.blit(title_surf, (rect.x + 14, rect.y + 12))

    def _draw_text(self, text: str, pos: Tuple[int, int], color=TEXT, font=None):
        font = font or self.font
        surf = font.render(text, False, color)
        self.screen.blit(surf, pos)

    def _truncate(self, text: str, max_chars: int) -> str:
        if len(text) <= max_chars:
            return text
        if max_chars <= 3:
            return text[:max_chars]
        return text[: max_chars - 3] + "..."

    def _update_progress_animation(self) -> None:
        if self._progress_ratio_display < self._progress_ratio_target:
            delta = self._progress_ratio_target - self._progress_ratio_display
            step = max(0.005, delta * 0.06)
            self._progress_ratio_display = min(
                self._progress_ratio_target, self._progress_ratio_display + step
            )
        else:
            self._progress_ratio_display = self._progress_ratio_target

    def run(self) -> None:
        running = True
        while running:
            self._poll_queue()
            self._build_buttons()
            self._update_progress_animation()
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
                "Carica PDF, analizza e converti OK/FORSE.",
                (20, 46),
                MUTED,
                self.font_small,
            )

            left_panel, right_panel = self._layout()
            self._draw_panel(left_panel, "Azioni")
            self._draw_panel(right_panel, "File caricati")

            is_busy = self._worker is not None and self._worker.is_alive()
            header_height = 48
            has_convertible = any(
                result.decision in {"OK", "FORSE"} for result in self.scan_results
            )

            for button in self._buttons:
                if button.label == "Converti":
                    button.enabled = (not is_busy) and has_convertible
                elif button.label != "Stop":
                    button.enabled = not is_busy
                if button.label == "Stop":
                    button.enabled = True
                button.draw(self.screen, self.font, mouse_pos)
                if pending_click and button.enabled and button.rect.collidepoint(click_pos):
                    button.on_click()

            # Debug toggle
            debug_rect = pygame.Rect(left_panel.x + 16, left_panel.bottom - 122, 18, 18)
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
            if not self.scan_results:
                self._draw_text(
                    "Nessun file caricato.",
                    (table_rect.x + 12, table_rect.y + 40),
                    MUTED,
                    self.font_small,
                )
            else:
                row_y = y + 26
                row_height = 28
                visible_rows = max(4, int((table_rect.height - 40) / row_height))
                start_idx = min(
                    self._triage_scroll,
                    max(0, len(self.scan_results) - visible_rows),
                )
                for result in self.scan_results[start_idx:start_idx + visible_rows]:
                    row_color = CARD
                    if not result.decision:
                        row_color = (242, 242, 242)
                    elif result.decision == "OK":
                        row_color = (236, 244, 236)
                    elif result.decision == "FORSE":
                        row_color = (247, 241, 232)
                    elif result.decision == "NO":
                        row_color = (248, 236, 236)
                    pygame.draw.rect(
                        self.screen,
                        row_color,
                        pygame.Rect(
                            table_rect.x + 6, row_y - 2, table_rect.width - 12, 26
                        ),
                        border_radius=6,
                    )
                    x = table_rect.x + 8
                    decision = result.decision or "in attesa"
                    parser = (
                        result.winner_parser
                        or result.final_parser
                        or result.parser
                        or result.suggested_profile
                        or ""
                    )
                    note = (
                        result.reasons
                        or result.failure_reason
                        or result.selection_reason
                        or ""
                    )
                    if not result.decision and not note:
                        note = "caricato"
                    cells = [
                        result.source_file or "",
                        decision,
                        parser,
                        note,
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
                ratio = min(1.0, self._progress_ratio_display)
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
                eta_label = f"ETA {self._format_eta()}"
                self._draw_text(
                    eta_label,
                    (bar_rect.right + 8, bar_rect.y - 2),
                    MUTED,
                    self.font_small,
                )

            pygame.display.flip()
            self.clock.tick(30)

        pygame.quit()


def start() -> None:
    App().run()
