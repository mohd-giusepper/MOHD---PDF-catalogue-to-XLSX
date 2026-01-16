import os
import queue
import threading
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Optional

from pdf2xlsx import config
from pdf2xlsx.core import auto_convert, debug_pack as debug_pack_utils, pipeline, profile_enrich, triage
from pdf2xlsx.io import triage_report
from pdf2xlsx.logging_setup import configure_logging


class App:
    def __init__(self) -> None:
        configure_logging("INFO")

        self.root = tk.Tk()
        self.root.title("Giuseppe Rubino - PDF to XLSX Converter")

        self.input_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.input_mode = tk.StringVar(value="folder")
        self.debug_var = tk.BooleanVar(value=False)
        self.ocr_var = tk.BooleanVar(value=False)
        self.status_var = tk.StringVar(value="Ready")
        self.summary_var = tk.StringVar(value="Nessuna scansione eseguita.")
        self._queue: queue.Queue = queue.Queue()
        self._worker: Optional[threading.Thread] = None
        self.triage_results = []
        self.stop_event = threading.Event()
        self.scan_done = False

        self._setup_style()
        self._build_ui()
        self._update_action_buttons()
        self._update_scan_label()
        self.input_mode.trace_add("write", lambda *_: self._update_scan_label())
        self._poll_queue()

    def _setup_style(self) -> None:
        self.root.option_add("*Font", ("Segoe UI", 10))
        style = ttk.Style()
        theme = "clam" if "clam" in style.theme_names() else style.theme_use()
        style.theme_use(theme)

        self._bg = "#f6f7fb"
        self._card = "#ffffff"
        self._text = "#0f172a"
        self._muted = "#6b7280"
        self._accent = "#2563eb"
        self._accent_hover = "#1d4ed8"

        self.root.configure(bg=self._bg)
        style.configure("TFrame", background=self._bg)
        style.configure("TLabel", background=self._bg, foreground=self._text)
        style.configure("TCheckbutton", background=self._bg, foreground=self._text)
        style.configure("TEntry", fieldbackground=self._card, foreground=self._text)
        style.configure("Accent.TButton", padding=(10, 6), background=self._accent, foreground="#ffffff")
        style.map(
            "Accent.TButton",
            background=[("active", self._accent_hover), ("pressed", self._accent_hover)],
            foreground=[("disabled", "#cbd5f5")],
        )
        style.configure(
            "Accent.Horizontal.TProgressbar",
            troughcolor=self._card,
            background=self._accent,
            thickness=8,
        )

    def _build_ui(self) -> None:
        form = ttk.Frame(self.root, padding=10)
        form.pack(fill="x")

        input_frame = ttk.LabelFrame(form, text="Input", padding=10)
        input_frame.pack(fill="x", pady=6)
        mode_row = ttk.Frame(input_frame)
        mode_row.pack(fill="x", pady=2)
        ttk.Label(mode_row, text="Modalita").pack(side="left")
        ttk.Radiobutton(
            mode_row, text="File singolo", variable=self.input_mode, value="file"
        ).pack(side="left", padx=6)
        ttk.Radiobutton(
            mode_row, text="Cartella", variable=self.input_mode, value="folder"
        ).pack(side="left", padx=6)
        self._add_row(input_frame, "Input", self.input_var, None)
        browse_row = ttk.Frame(input_frame)
        browse_row.pack(fill="x", pady=2)
        ttk.Button(browse_row, text="Sfoglia file", command=self._browse_input_file).pack(
            side="left"
        )
        ttk.Button(
            browse_row, text="Sfoglia cartella", command=self._browse_input_folder
        ).pack(side="left", padx=6)

        output_frame = ttk.LabelFrame(form, text="Output", padding=10)
        output_frame.pack(fill="x", pady=6)
        self._add_row(output_frame, "Output XLSX", self.output_var, self._browse_output)

        debug_frame = ttk.Frame(form)
        debug_frame.pack(fill="x", pady=4)
        ttk.Checkbutton(
            debug_frame, text="Debug JSON", variable=self.debug_var
        ).pack(side="left")
        ttk.Checkbutton(
            debug_frame,
            text="OCR fallback (solo pagine scansione)",
            variable=self.ocr_var,
        ).pack(side="left", padx=8)

        button_frame = ttk.LabelFrame(form, text="Azioni", padding=10)
        button_frame.pack(fill="x", pady=6)
        self.scan_button = ttk.Button(
            button_frame,
            text="Scansiona cartella",
            command=self._run_triage_scan,
        )
        self.scan_button.pack(side="left")
        self.convert_button = ttk.Button(
            button_frame,
            text="Converti solo OK",
            command=self._run_auto_convert,
            style="Accent.TButton",
        )
        self.convert_button.pack(side="right")
        self.enrich_button = ttk.Button(
            button_frame,
            text="Arricchisci profili",
            command=self._run_profile_enrich,
        )
        self.enrich_button.pack(side="right", padx=6)
        self.stop_button = ttk.Button(
            button_frame,
            text="Stop",
            command=self._request_stop,
        )
        self.stop_button.pack(side="right", padx=6)
        self.stop_button.configure(state="disabled")

        progress_frame = ttk.Frame(self.root, padding=(10, 0, 10, 6))
        progress_frame.pack(fill="x")
        self.progress = ttk.Progressbar(
            progress_frame,
            orient="horizontal",
            mode="determinate",
            style="Accent.Horizontal.TProgressbar",
        )
        self.progress.pack(fill="x")

        triage_frame = ttk.LabelFrame(self.root, text="Risultati scan", padding=(10, 6))
        triage_frame.pack(fill="both", expand=False)
        ttk.Label(triage_frame, textvariable=self.summary_var).pack(anchor="w")
        self.triage_tree = ttk.Treeview(
            triage_frame,
            columns=(
                "file",
                "decision",
                "profile",
                "score",
                "status",
                "parser",
                "attempts",
                "reason",
            ),
            show="headings",
            height=6,
        )
        self.triage_tree.heading("file", text="File")
        self.triage_tree.heading("decision", text="Decisione")
        self.triage_tree.heading("profile", text="Profilo suggerito")
        self.triage_tree.heading("score", text="Score")
        self.triage_tree.heading("status", text="Status")
        self.triage_tree.heading("parser", text="Parser")
        self.triage_tree.heading("attempts", text="Tentativi")
        self.triage_tree.heading("reason", text="Motivo")
        self.triage_tree.column("file", width=200, anchor="w")
        self.triage_tree.column("decision", width=70, anchor="center")
        self.triage_tree.column("profile", width=130, anchor="center")
        self.triage_tree.column("score", width=60, anchor="center")
        self.triage_tree.column("status", width=120, anchor="center")
        self.triage_tree.column("parser", width=120, anchor="center")
        self.triage_tree.column("attempts", width=80, anchor="center")
        self.triage_tree.column("reason", width=200, anchor="w")
        self.triage_tree.tag_configure("ok", foreground="#15803d")
        self.triage_tree.tag_configure("maybe", foreground="#b45309")
        self.triage_tree.tag_configure("no", foreground="#b91c1c")
        self.triage_tree.tag_configure("converted", foreground="#1d4ed8")
        tree_scroll = ttk.Scrollbar(triage_frame, orient="vertical", command=self.triage_tree.yview)
        self.triage_tree.configure(yscrollcommand=tree_scroll.set)
        self.triage_tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

        log_frame = ttk.Frame(self.root, padding=(10, 6))
        log_frame.pack(fill="both", expand=True)
        ttk.Label(log_frame, text="Log").pack(anchor="w")
        self.log_area = scrolledtext.ScrolledText(
            log_frame,
            height=8,
            state="disabled",
            bg=self._card,
            fg=self._text,
            insertbackground=self._text,
            relief="flat",
        )
        self.log_area.pack(fill="both", expand=True)

        status_frame = ttk.Frame(self.root, padding=(10, 6))
        status_frame.pack(fill="x")
        ttk.Label(status_frame, textvariable=self.status_var).pack(anchor="w")

    def _add_row(
        self,
        parent: tk.Misc,
        label: str,
        variable: tk.StringVar,
        browse_callback,
    ) -> None:
        row = ttk.Frame(parent)
        row.pack(fill="x", pady=4)
        ttk.Label(row, text=label, width=12, anchor="w").pack(side="left")
        entry = ttk.Entry(row, textvariable=variable)
        entry.pack(side="left", fill="x", expand=True)
        if browse_callback:
            ttk.Button(row, text="Browse", command=browse_callback).pack(
                side="left", padx=4
            )

    def _browse_input(self) -> None:
        return

    def _browse_input_file(self) -> None:
        path = filedialog.askopenfilename(
            initialdir=config.INPUT_DIR,
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if path:
            self.input_mode.set("file")
            self.input_var.set(path)
            if not self.output_var.get().strip():
                output_dir = os.path.join(os.getcwd(), config.OUTPUT_DIR)
                os.makedirs(output_dir, exist_ok=True)
                stem = os.path.splitext(os.path.basename(path))[0]
                self.output_var.set(os.path.join(output_dir, f"{stem}.xlsx"))

    def _browse_input_folder(self) -> None:
        path = filedialog.askdirectory(initialdir=config.INPUT_DIR)
        if path:
            self.input_mode.set("folder")
            self.input_var.set(path)

    def _browse_output(self) -> None:
        path = filedialog.asksaveasfilename(
            initialdir=config.OUTPUT_DIR,
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
        )
        if path:
            self.output_var.set(path)

    def _log(self, message: str) -> None:
        self.log_area.configure(state="normal")
        self.log_area.insert(tk.END, message + "\n")
        self.log_area.configure(state="disabled")
        self.log_area.see(tk.END)

    def _run_job(self) -> None:
        input_path = self.input_var.get().strip()
        output_path = self.output_var.get().strip()

        if not input_path:
            messagebox.showerror("Missing input", "Select input and output files.")
            return
        if not os.path.exists(input_path):
            messagebox.showerror("Missing input", "Input PDF not found.")
            return
        if not output_path:
            output_dir = os.path.join(os.getcwd(), config.OUTPUT_DIR)
            os.makedirs(output_dir, exist_ok=True)
            stem = os.path.splitext(os.path.basename(input_path))[0]
            output_path = os.path.join(output_dir, f"{stem}.xlsx")
            self.output_var.set(output_path)
        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        debug_json = None
        if self.debug_var.get():
            debug_json = output_path + config.DEBUG_JSON_SUFFIX

        self._set_busy(True)
        self.progress["value"] = 0
        self.status_var.set("Running...")
        self._log("Starting conversion...")

        args = {
            "input_pdf": input_path,
            "output_xlsx": output_path,
            "pages": None,
            "debug_json": debug_json,
            "parser_name": config.DEFAULT_PARSER,
            "ocr": self.ocr_var.get(),
            "progress_callback": self._progress_callback,
            "currency_only": config.TARGET_CURRENCY,
        }

        self._worker = threading.Thread(
            target=self._run_job_background, args=(args,), daemon=True
        )
        self._worker.start()

    def _run_job_background(self, args: dict) -> None:
        try:
            report = pipeline.run_pipeline(**args)
        except Exception as exc:
            self._queue.put(("error", str(exc)))
        else:
            self._queue.put(("done", report, args.get("debug_json")))

    def _run_triage_scan(self) -> None:
        target_path, is_file = self._resolve_input_target()
        if is_file and not os.path.isfile(target_path):
            messagebox.showerror("Missing input", f"Input file not found: {target_path}")
            return
        if not is_file and not os.path.isdir(target_path):
            messagebox.showerror("Missing input", f"Input folder not found: {target_path}")
            return
        output_dir = self._resolve_output_dir()
        os.makedirs(output_dir, exist_ok=True)
        report_path = os.path.join(output_dir, "triage_report.xlsx")

        self._set_busy(True)
        self.progress["value"] = 0
        self.status_var.set("Scanning...")
        label = "file" if is_file else "folder"
        self._log(f"Scanning {label}: {target_path}")
        self.stop_event.clear()

        args = {
            "target_path": target_path,
            "is_file": is_file,
            "output_dir": output_dir,
            "report_path": report_path,
            "ocr": self.ocr_var.get(),
            "debug_pack_dir": config.DEBUG_PACK_DIR,
            "debug_level": "light",
        }
        self._worker = threading.Thread(
            target=self._run_triage_scan_background, args=(args,), daemon=True
        )
        self._worker.start()

    def _run_triage_scan_background(self, args: dict) -> None:
        results = []
        debug_pack = None
        input_files = []
        try:
            if args["is_file"]:
                input_files = [Path(args["target_path"])]
            else:
                input_files = sorted(Path(args["target_path"]).glob("*.pdf"))
            mode = "single" if len(input_files) == 1 else "batch"
            debug_pack = debug_pack_utils.DebugPack(
                base_dir=args["debug_pack_dir"],
                input_files=input_files,
                mode=mode,
                level=args["debug_level"],
                target_currency=config.TARGET_CURRENCY,
            )
            self._queue.put(("log", f"Debug pack: {debug_pack.root}"))

            if args["is_file"]:
                results = [triage.scan_pdf(args["target_path"], ocr=args["ocr"])]
            else:
                results = triage.scan_folder(
                    args["target_path"],
                    ocr=args["ocr"],
                    progress_callback=self._triage_progress_callback,
                    should_stop=self.stop_event.is_set,
                )
            triage_report.write_triage_report(results, args["report_path"])
            if not args["is_file"]:
                pdf_count = len(
                    [
                        name
                        for name in os.listdir(args["target_path"])
                        if name.lower().endswith(".pdf")
                    ]
                )
                if len(results) != pdf_count:
                    self._queue.put(
                        (
                            "log",
                            "WARNING: triage report rows "
                            f"({len(results)}) != scanned PDFs ({pdf_count})",
                        )
                    )
            if debug_pack:
                for result in results:
                    debug_pack.write_pdf_pack(result)
                debug_pack.write_triage_report(results)
        except Exception as exc:
            if debug_pack:
                debug_pack.write_triage_report(results)
                partial = len(results) != len(input_files)
                debug_pack.finalize(results, partial_run=partial or True)
            self._queue.put(("error", str(exc)))
            return
        finally:
            if debug_pack:
                debug_pack.write_triage_report(results)
                partial = self.stop_event.is_set() or len(results) != len(input_files)
                debug_pack.finalize(results, partial_run=partial)
        self._queue.put(
            ("triage_done", results, args["report_path"], self.stop_event.is_set())
        )

    def _run_auto_convert(self) -> None:
        if not self.scan_done:
            messagebox.showerror("Missing scan", "Run 'Scansiona cartella' first.")
            return
        ok_targets = [
            result for result in self.triage_results if result.decision == "OK"
        ]
        if not ok_targets:
            messagebox.showinfo("No OK files", "No OK files to convert.")
            return
        output_dir = self._resolve_output_dir()
        os.makedirs(output_dir, exist_ok=True)
        report_path = os.path.join(output_dir, "triage_report.xlsx")

        self._set_busy(True)
        self.progress["value"] = 0
        self.status_var.set("Converting (auto)...")
        self._log("Auto converting OK files...")
        self.stop_event.clear()

        args = {
            "targets": ok_targets,
            "output_dir": output_dir,
            "report_path": report_path,
            "ocr": self.ocr_var.get(),
            "debug_pack_dir": config.DEBUG_PACK_DIR,
            "debug_level": "light",
        }
        self._worker = threading.Thread(
            target=self._run_auto_convert_background, args=(args,), daemon=True
        )
        self._worker.start()

    def _run_auto_convert_background(self, args: dict) -> None:
        results = []
        debug_pack = None
        input_files = [Path(result.source_path) for result in args["targets"]]
        try:
            mode = "single" if len(input_files) == 1 else "batch"
            debug_pack = debug_pack_utils.DebugPack(
                base_dir=args["debug_pack_dir"],
                input_files=input_files,
                mode=mode,
                level=args["debug_level"],
                target_currency=config.TARGET_CURRENCY,
            )
            self._queue.put(("log", f"Debug pack: {debug_pack.root}"))

            total = len(args["targets"])
            for idx, result in enumerate(args["targets"], start=1):
                if self.stop_event.is_set():
                    break
                self._triage_progress_callback(idx - 1, total, result.source_file)
                converted = auto_convert.run_auto_for_pdf(
                    pdf_path=result.source_path,
                    output_dir=args["output_dir"],
                    ocr=args["ocr"],
                    currency=config.TARGET_CURRENCY,
                    currency_only=None,
                )
                results.append(converted)
                triage_report.write_triage_report(results, args["report_path"])
                if debug_pack:
                    debug_pack.write_pdf_pack(converted)
                    debug_pack.write_triage_report(results)
                self._triage_progress_callback(idx, total, result.source_file)
        except Exception as exc:
            if debug_pack:
                debug_pack.write_triage_report(results)
                partial = len(results) != len(input_files)
                debug_pack.finalize(results, partial_run=partial or True)
            self._queue.put(("error", str(exc)))
            return
        finally:
            if debug_pack:
                debug_pack.write_triage_report(results)
                partial = self.stop_event.is_set() or len(results) != len(input_files)
                debug_pack.finalize(results, partial_run=partial)

        self._queue.put(
            (
                "triage_convert_done",
                results,
                args["report_path"],
                self._summarize_conversions(results),
                self.stop_event.is_set(),
            )
        )

    def _progress_callback(
        self, processed: int, total: int, page_number: int, pdf_total: int
    ) -> None:
        self._queue.put(
            ("progress", processed, total, page_number, pdf_total)
        )

    def _triage_progress_callback(self, processed: int, total: int, filename: str) -> None:
        self._queue.put(("triage_progress", processed, total, filename))

    def _resolve_input_target(self) -> tuple[str, bool]:
        input_path = self.input_var.get().strip()
        if self.input_mode.get() == "file":
            if input_path:
                return input_path, True
            return "", True
        if input_path:
            return input_path, False
        return os.path.join(os.getcwd(), config.INPUT_DIR), False

    def _resolve_output_dir(self) -> str:
        output_path = self.output_var.get().strip()
        if output_path:
            if output_path.lower().endswith(".xlsx"):
                return os.path.dirname(output_path)
            return output_path
        return os.path.join(os.getcwd(), config.OUTPUT_DIR)

    def _set_busy(self, busy: bool) -> None:
        state = "disabled" if busy else "normal"
        self.scan_button.configure(state=state)
        if busy:
            self.convert_button.configure(state="disabled")
            self.enrich_button.configure(state="disabled")
        else:
            self._update_action_buttons()
        self.stop_button.configure(state="normal" if busy else "disabled")

    def _request_stop(self) -> None:
        if not self.stop_event.is_set():
            self.stop_event.set()
            self.status_var.set("Stopping...")
            self._log("Stop requested.")

    def _summarize_conversions(self, results) -> dict:
        summary = {"processed_ok": 0, "failed": 0, "skipped": 0}
        for result in results:
            status = result.final_status or ""
            if status.startswith("CONVERTED"):
                summary["processed_ok"] += 1
            elif status.startswith("FAILED"):
                summary["failed"] += 1
            elif status.startswith("SKIPPED"):
                summary["skipped"] += 1
        return summary

    def _format_summary(self) -> str:
        if not self.triage_results:
            return "Nessun risultato."
        counts = {"OK": 0, "FORSE": 0, "NO": 0}
        converted = 0
        partial = 0
        failed = 0
        for result in self.triage_results:
            counts[result.decision] = counts.get(result.decision, 0) + 1
            status = result.final_status or ""
            if status.startswith("CONVERTED"):
                converted += 1
            elif status.startswith("PARTIAL"):
                partial += 1
            elif status.startswith("FAILED"):
                failed += 1
        return (
            f"OK={counts['OK']} | FORSE={counts['FORSE']} | NO={counts['NO']} | "
            f"CONVERTED={converted} | PARTIAL={partial} | FAILED={failed}"
        )

    def _render_triage_results(self) -> None:
        for item in self.triage_tree.get_children():
            self.triage_tree.delete(item)
        for result in self.triage_results:
            score = f"{result.support_score:.1f}" if result.support_score else "0.0"
            attempts_count = str(result.attempts_count or 0)
            status = result.final_status or ""
            parser = result.final_parser or ""
            reason = result.failure_reason or result.selection_reason or ""
            tag = ""
            if status.startswith("CONVERTED") or status.startswith("PARTIAL"):
                tag = "converted"
            elif result.decision == "OK":
                tag = "ok"
            elif result.decision == "FORSE":
                tag = "maybe"
            elif result.decision == "NO":
                tag = "no"
            self.triage_tree.insert(
                "",
                "end",
                iid=result.source_file,
                values=(
                    result.source_file,
                    result.decision,
                    result.suggested_profile,
                    score,
                    status,
                    parser,
                    attempts_count,
                    reason,
                ),
                tags=(tag,) if tag else (),
            )
        self.summary_var.set(self._format_summary())

    def _update_action_buttons(self) -> None:
        if not self.scan_done:
            self.convert_button.configure(state="disabled")
            self.enrich_button.configure(state="disabled")
            return
        has_ok = any(result.decision == "OK" for result in self.triage_results)
        has_unknown = any(
            result.suggested_profile == "unknown"
            or result.decision in {"FORSE", "NO"}
            for result in self.triage_results
        )
        self.convert_button.configure(state="normal" if has_ok else "disabled")
        self.enrich_button.configure(state="normal" if has_unknown else "disabled")

    def _update_scan_label(self) -> None:
        label = "Scansiona file" if self.input_mode.get() == "file" else "Scansiona cartella"
        self.scan_button.configure(text=label)

    def _run_profile_enrich(self) -> None:
        if not self.scan_done:
            messagebox.showerror("Missing scan", "Run 'Scansiona cartella' first.")
            return
        candidates = [
            result
            for result in self.triage_results
            if result.suggested_profile == "unknown"
            or result.decision in {"FORSE", "NO"}
        ]
        if not candidates:
            messagebox.showinfo("No candidates", "No PDFs need profile enrichment.")
            return

        self._set_busy(True)
        self.progress["value"] = 0
        self.status_var.set("Arricchimento profili...")
        self._log("Generating profile suggestions...")
        self.stop_event.clear()

        args = {"targets": candidates, "ocr": self.ocr_var.get()}
        self._worker = threading.Thread(
            target=self._run_profile_enrich_background, args=(args,), daemon=True
        )
        self._worker.start()

    def _run_profile_enrich_background(self, args: dict) -> None:
        try:
            results = []
            total = len(args["targets"])
            for idx, result in enumerate(args["targets"], start=1):
                if self.stop_event.is_set():
                    break
                self._triage_progress_callback(idx - 1, total, result.source_file)
                profile, stats = profile_enrich.suggest_profile_for_pdf(
                    result.source_path, ocr=args["ocr"]
                )
                if profile.get("fields"):
                    profile_id = profile_enrich.build_profile_id(result.source_file)
                    output_path = profile_enrich.write_profile(profile_id, profile)
                    results.append((result.source_file, output_path, stats))
                self._triage_progress_callback(idx, total, result.source_file)
        except Exception as exc:
            self._queue.put(("error", str(exc)))
            return

        self._queue.put(("enrich_done", results, self.stop_event.is_set()))

    def _poll_queue(self) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            kind = item[0]
            if kind == "progress":
                _, processed, total, page_number, pdf_total = item
                if total:
                    self.progress["maximum"] = total
                    self.progress["value"] = processed
                self.status_var.set(f"Processing page {page_number} of {pdf_total}")
            elif kind == "triage_progress":
                _, processed, total, filename = item
                if total:
                    self.progress["maximum"] = total
                    self.progress["value"] = processed
                self.status_var.set(f"Processing {filename} ({processed}/{total})")
            elif kind == "triage_done":
                results, report_path, was_stopped = item[1], item[2], item[3]
                self.triage_results = results
                self._render_triage_results()
                self._log(f"Triage report: {report_path}")
                self.status_var.set("Triage stopped" if was_stopped else "Triage completed")
                self.scan_done = True
                self._set_busy(False)
            elif kind == "triage_convert_progress":
                _, processed, total, filename = item
                if total:
                    self.progress["maximum"] = total
                    self.progress["value"] = processed
                self.status_var.set(f"Converting {filename} ({processed}/{total})")
            elif kind == "triage_convert_done":
                results = item[1]
                report_path = item[2]
                summary = item[3]
                was_stopped = item[4]
                self.triage_results = results
                self._render_triage_results()
                self._log(f"Triage report: {report_path}")
                self._log(
                    "Conversion summary: "
                    f"ok={summary.get('processed_ok', 0)} "
                    f"failed={summary.get('failed', 0)} "
                    f"skipped={summary.get('skipped', 0)}"
                )
                self.status_var.set("Stopped" if was_stopped else "Completed")
                self._set_busy(False)
            elif kind == "enrich_done":
                results = item[1]
                was_stopped = item[2]
                if results:
                    for source_file, output_path, stats in results:
                        self._log(
                            f"Profile suggestion for {source_file}: {output_path} "
                            f"(pages={stats.get('pages_used')}, fields={stats.get('fields_suggested')})"
                        )
                else:
                    self._log("No profile suggestions created.")
                self.status_var.set("Stopped" if was_stopped else "Profile enrichment done")
                self._set_busy(False)
            elif kind == "log":
                self._log(item[1])
            elif kind == "done":
                report = item[1]
                debug_json = item[2]
                self._log(f"Done. Rows: {len(report.rows)}")
                self._log(
                    f"Pages OCR used: {report.pages_ocr_used} | Needs review: {report.rows_needs_review}"
                )
                if debug_json:
                    self._log(f"Debug JSON: {debug_json}")
                self.status_var.set("Completed")
                self._set_busy(False)
            elif kind == "error":
                message = item[1]
                self._log(f"Error: {message}")
                self.status_var.set("Error")
                self._set_busy(False)
                messagebox.showerror("Error", message)
        self.root.after(100, self._poll_queue)

    def start(self) -> None:
        self.root.mainloop()


def start() -> None:
    App().start()
