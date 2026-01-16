import os
import queue
import threading
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import List, Optional, Tuple

from pdf2xlsx import config
from pdf2xlsx.core import auto_convert, triage
from pdf2xlsx.io.run_debug import RunDebugCollector
from pdf2xlsx.logging_setup import configure_logging
from pdf2xlsx.models import TriageResult


class App:
    def __init__(self) -> None:
        configure_logging("INFO")

        self.root = tk.Tk()
        self.root.title("PDF Catalog Parser")
        self.root.minsize(980, 640)

        self._queue: queue.Queue = queue.Queue()
        self._worker: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        self.scan_results: List[TriageResult] = []
        self._row_items: List[str] = []
        self._progress_target = 0
        self._progress_value = 0
        self._progress_animating = False

        self.debug_var = tk.BooleanVar(value=config.DEBUG_JSON_DEFAULT)
        self.status_var = tk.StringVar(value="Seleziona file o cartella per iniziare.")
        self._run_debug: Optional[RunDebugCollector] = None

        self._setup_style()
        self._build_ui()
        self._poll_queue()

    def _setup_style(self) -> None:
        self.root.option_add("*Font", ("Segoe UI", 10))
        style = ttk.Style()
        theme = "clam" if "clam" in style.theme_names() else style.theme_use()
        style.theme_use(theme)

        self._bg = "#f5f6f8"
        self._card = "#ffffff"
        self._text = "#111827"
        self._muted = "#6b7280"
        self._accent = "#111827"
        self._accent_hover = "#1f2937"

        self.root.configure(bg=self._bg)
        style.configure("TFrame", background=self._bg)
        style.configure("TLabel", background=self._bg, foreground=self._text)
        style.configure("TCheckbutton", background=self._bg, foreground=self._text)
        style.configure("TEntry", fieldbackground=self._card, foreground=self._text)
        style.configure("Card.TFrame", background=self._card)
        style.configure("Card.TLabelframe", background=self._card)
        style.configure("Card.TLabelframe.Label", background=self._bg, foreground=self._text)
        style.configure(
            "Accent.TButton",
            padding=(10, 6),
            background=self._accent,
            foreground="#ffffff",
        )
        style.map(
            "Accent.TButton",
            background=[("active", self._accent_hover), ("pressed", self._accent_hover)],
            foreground=[("disabled", "#d1d5db")],
        )
        style.configure(
            "Accent.Horizontal.TProgressbar",
            troughcolor=self._card,
            background="#2ea043",
            thickness=8,
        )

    def _build_ui(self) -> None:
        header = ttk.Frame(self.root, padding=(16, 12, 16, 6))
        header.pack(fill="x")
        ttk.Label(header, text="PDF Catalog Parser", font=("Segoe UI", 14, "bold")).pack(
            anchor="w"
        )
        ttk.Label(
            header,
            text="Carica PDF, analizza e converti OK/FORSE.",
            foreground=self._muted,
        ).pack(anchor="w")

        main = ttk.Frame(self.root, padding=(16, 6, 16, 8))
        main.pack(fill="both", expand=True)

        left = ttk.Frame(main)
        left.pack(side="left", fill="y", padx=(0, 12))

        right = ttk.Frame(main)
        right.pack(side="right", fill="both", expand=True)

        actions = ttk.LabelFrame(left, text="Azioni", padding=12)
        actions.pack(fill="y", expand=False)

        self.load_single_btn = ttk.Button(
            actions, text="Carica PDF singolo", command=self._load_single
        )
        self.load_single_btn.pack(fill="x", pady=(0, 6))
        self.load_folder_btn = ttk.Button(
            actions, text="Carica cartella input", command=self._load_folder
        )
        self.load_folder_btn.pack(fill="x", pady=(0, 6))
        self.convert_btn = ttk.Button(
            actions, text="Converti", command=self._convert_loaded, style="Accent.TButton"
        )
        self.convert_btn.pack(fill="x", pady=(0, 10))

        self.stop_btn = ttk.Button(actions, text="Stop", command=self._request_stop)
        self.stop_btn.pack(fill="x")

        ttk.Separator(actions).pack(fill="x", pady=10)
        ttk.Checkbutton(
            actions, text="Debug", variable=self.debug_var
        ).pack(anchor="w")

        results = ttk.LabelFrame(right, text="File caricati", padding=8)
        results.pack(fill="both", expand=True)
        self.triage_tree = ttk.Treeview(
            results,
            columns=("file", "decision", "parser", "note"),
            show="headings",
        )
        self.triage_tree.heading("file", text="File")
        self.triage_tree.heading("decision", text="Decisione")
        self.triage_tree.heading("parser", text="Parser")
        self.triage_tree.heading("note", text="Nota")
        self.triage_tree.column("file", width=320, anchor="w")
        self.triage_tree.column("decision", width=90, anchor="center")
        self.triage_tree.column("parser", width=140, anchor="center")
        self.triage_tree.column("note", width=260, anchor="w")
        self.triage_tree.tag_configure("ok", foreground="#15803d")
        self.triage_tree.tag_configure("maybe", foreground="#b45309")
        self.triage_tree.tag_configure("no", foreground="#b91c1c")
        self.triage_tree.tag_configure("pending", foreground=self._muted)
        tree_scroll = ttk.Scrollbar(results, orient="vertical", command=self.triage_tree.yview)
        self.triage_tree.configure(yscrollcommand=tree_scroll.set)
        self.triage_tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

        log_frame = ttk.LabelFrame(self.root, text="Log", padding=8)
        log_frame.pack(fill="both", padx=16, pady=(0, 8))
        self.log_area = scrolledtext.ScrolledText(
            log_frame,
            height=6,
            state="disabled",
            bg=self._card,
            fg=self._text,
            insertbackground=self._text,
            relief="flat",
        )
        self.log_area.pack(fill="both", expand=True)

        footer = ttk.Frame(self.root, padding=(16, 0, 16, 12))
        footer.pack(fill="x")
        self.progress = ttk.Progressbar(
            footer,
            orient="horizontal",
            mode="determinate",
            style="Accent.Horizontal.TProgressbar",
        )
        self.progress.pack(fill="x", pady=(0, 6))
        ttk.Label(footer, textvariable=self.status_var, foreground=self._muted).pack(
            anchor="w"
        )

        self._set_busy(False)

    def _log(self, message: str) -> None:
        self.log_area.configure(state="normal")
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.configure(state="disabled")
        self.log_area.see(tk.END)

    def _set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        self.load_single_btn.configure(state=state)
        self.load_folder_btn.configure(state=state)
        self.convert_btn.configure(state=state if self._has_convertible() else "disabled")
        self.stop_btn.configure(state="normal" if busy else "disabled")

    def _has_convertible(self) -> bool:
        return any(result.decision in {"OK", "FORSE"} for result in self.scan_results)

    def _request_stop(self) -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()
            self.status_var.set("Stop richiesto...")
            self._log("Stop richiesto.")

    def _pick_file(self) -> str:
        return filedialog.askopenfilename(
            initialdir=config.INPUT_DIR,
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )

    def _pick_folder(self) -> str:
        return filedialog.askdirectory(initialdir=config.INPUT_DIR)

    def _load_single(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self._pick_file()
        if not path:
            return
        pdf_path = Path(path)
        self._start_scan([(pdf_path, pdf_path.name)])

    def _load_folder(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        path = self._pick_folder()
        if not path:
            return
        folder = Path(path)
        if not folder.exists():
            messagebox.showerror("Errore", "Cartella input non trovata.")
            return
        pdfs = sorted(folder.rglob("*.pdf"))
        if not pdfs:
            messagebox.showerror("Errore", "Nessun PDF trovato nella cartella.")
            return
        targets = [(pdf_path, str(pdf_path.relative_to(folder))) for pdf_path in pdfs]
        self._start_scan(targets)

    def _start_scan(self, targets: List[Tuple[Path, str]]) -> None:
        if self._worker and self._worker.is_alive():
            return
        if not targets:
            messagebox.showerror("Errore", "Nessun PDF selezionato.")
            return
        self.stop_event.clear()
        self._run_debug = RunDebugCollector.start(
            input_root=str(config.INPUT_DIR), run_type="scan"
        )
        self.scan_results = []
        self._row_items = []
        self._clear_tree()
        self._reset_progress(len(targets))
        self.status_var.set(f"Caricati {len(targets)} file. Avvio analisi...")
        self._log(f"Avvio analisi su {len(targets)} file.")

        for idx, (path, display_name) in enumerate(targets):
            result = TriageResult(source_file=display_name, source_path=str(path))
            self.scan_results.append(result)
            item_id = f"row-{idx}"
            self._row_items.append(item_id)
            self.triage_tree.insert(
                "",
                "end",
                iid=item_id,
                values=(display_name, "in attesa", "", "caricato"),
                tags=("pending",),
            )

        self._set_busy(True)
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
            self._queue.put(("progress", idx - 1, total, display_name, "scan"))
            cached_pages = []
            try:
                triage_result, cached_pages = triage.scan_pdf_cached(
                    str(pdf_path), ocr=False
                )
                triage_result.source_file = display_name
                triage_result.source_path = str(pdf_path)
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
            self._queue.put(("progress", idx, total, display_name, "scan"))
            if self._run_debug:
                self._run_debug.add_pdf(
                    str(pdf_path),
                    triage_result,
                    cached_pages,
                    reason=triage_result.reasons,
                )
        self._queue.put(("scan_done", results, self.stop_event.is_set()))

    def _convert_loaded(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        targets = [
            (idx, result)
            for idx, result in enumerate(self.scan_results)
            if result.decision in {"OK", "FORSE"}
        ]
        if not targets:
            self.status_var.set("Nessun file convertibile.")
            return
        self.stop_event.clear()
        self._run_debug = RunDebugCollector.start(
            input_root=str(config.INPUT_DIR), run_type="convert"
        )
        self._reset_progress(len(targets))
        self.status_var.set(f"Conversione {len(targets)} file...")
        self._log(f"Conversione {len(targets)} file.")
        self._set_busy(True)
        self._worker = threading.Thread(
            target=self._run_convert_background, args=(targets,), daemon=True
        )
        self._worker.start()

    def _run_convert_background(self, targets: List[Tuple[int, TriageResult]]) -> None:
        total = len(targets)
        converted = 0
        for idx, (row_index, result) in enumerate(targets, start=1):
            if self.stop_event.is_set():
                break
            self._queue.put(("progress", idx - 1, total, result.source_file, "convert"))
            converted_result = auto_convert.run_auto_for_pdf(
                pdf_path=result.source_path,
                output_dir=str(Path(config.OUTPUT_DIR)),
                ocr=False,
                run_debug=self._run_debug,
            )
            converted_result.source_file = result.source_file
            converted_result.source_path = result.source_path
            self._queue.put(("convert_result", row_index, converted_result))
            if converted_result.output_path:
                self._queue.put(
                    ("log", f"Output: {converted_result.output_path}")
                )
            else:
                self._queue.put(
                    ("log", f"Nessun output salvato per {converted_result.source_file}")
                )
            converted += 1
            self._queue.put(("progress", idx, total, result.source_file, "convert"))
        self._queue.put(("convert_done", converted, self.stop_event.is_set()))

    def _clear_tree(self) -> None:
        for item in self.triage_tree.get_children():
            self.triage_tree.delete(item)

    def _update_tree_row(self, row_index: int, result: TriageResult) -> None:
        if row_index >= len(self._row_items):
            return
        decision = result.decision or "in attesa"
        parser = (
            result.winner_parser
            or result.final_parser
            or result.parser
            or result.suggested_profile
            or ""
        )
        note = result.reasons or result.failure_reason or result.selection_reason or ""
        if result.final_status:
            note = f"{result.final_status} | {note}" if note else result.final_status
        if not result.decision and not note:
            note = "caricato"
        tag = "pending"
        if result.decision == "OK":
            tag = "ok"
        elif result.decision == "FORSE":
            tag = "maybe"
        elif result.decision == "NO":
            tag = "no"
        item_id = self._row_items[row_index]
        self.triage_tree.item(
            item_id, values=(result.source_file, decision, parser, note), tags=(tag,)
        )

    def _set_progress_target(self, value: int, total: int) -> None:
        self.progress["maximum"] = max(1, total)
        self._progress_target = min(value, total)
        if not self._progress_animating:
            self._progress_animating = True
            self._animate_progress()

    def _animate_progress(self) -> None:
        if self._progress_value < self._progress_target:
            self._progress_value += 1
            self.progress["value"] = self._progress_value
            self.root.after(30, self._animate_progress)
            return
        self._progress_value = self._progress_target
        self.progress["value"] = self._progress_value
        self._progress_animating = False

    def _reset_progress(self, total: int) -> None:
        self.progress["maximum"] = max(1, total)
        self._progress_target = 0
        self._progress_value = 0
        self.progress["value"] = 0
        self._progress_animating = False

    def _write_summary(self) -> None:
        if not self._run_debug:
            return
        debug_dir = Path(config.DEBUG_PACK_DIR)
        debug_dir.mkdir(parents=True, exist_ok=True)
        self._run_debug.write(str(debug_dir))
        self._run_debug = None
        self._log(f"Report: {output_path}")

    def _poll_queue(self) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            kind = item[0]
            if kind == "progress":
                _, processed, total, filename, stage = item
                self._set_progress_target(processed, total)
                if stage == "convert":
                    self.status_var.set(f"Conversione {filename} ({processed}/{total})")
                else:
                    self.status_var.set(f"Analisi {filename} ({processed}/{total})")
            elif kind == "scan_result":
                row_index, result = item[1], item[2]
                if 0 <= row_index < len(self.scan_results):
                    self.scan_results[row_index] = result
                    self._update_tree_row(row_index, result)
            elif kind == "scan_done":
                results, partial = item[1], item[2]
                if len(results) == len(self.scan_results):
                    self.scan_results = results
                self._write_summary()
                self.status_var.set("Analisi interrotta." if partial else "Analisi completata.")
                self._set_busy(False)
            elif kind == "convert_result":
                row_index, result = item[1], item[2]
                if 0 <= row_index < len(self.scan_results):
                    self.scan_results[row_index] = result
                    self._update_tree_row(row_index, result)
            elif kind == "convert_done":
                total, partial = item[1], item[2]
                self._write_summary()
                msg = (
                    f"Conversione interrotta ({total} completati)"
                    if partial
                    else f"Conversione completata ({total} completati)"
                )
                self.status_var.set(msg)
                zero_rows = [
                    result.source_file
                    for result in self.scan_results
                    if result.decision in {"OK", "FORSE"}
                    and (result.rows_exported or 0) == 0
                    and not result.output_path
                ]
                if zero_rows:
                    names = ", ".join(zero_rows[:5])
                    suffix = "..." if len(zero_rows) > 5 else ""
                    notice = (
                        "Alcuni file contengono 0 righe e non sono stati salvati "
                        f"come XLSX: {names}{suffix}"
                    )
                    self._log(notice)
                    messagebox.showinfo("Nessun output", notice)
                self._set_busy(False)
            elif kind == "error":
                message = item[1]
                self._log(f"Errore: {message}")
                self.status_var.set("Errore")
                self._set_busy(False)
                messagebox.showerror("Errore", message)
        self.root.after(100, self._poll_queue)

    def start(self) -> None:
        self.root.mainloop()


def start() -> None:
    App().start()
