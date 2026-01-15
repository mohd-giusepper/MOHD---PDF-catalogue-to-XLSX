import os
import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Optional

from pdf2xlsx import config
from pdf2xlsx.core import pipeline
from pdf2xlsx.logging_setup import configure_logging


class App:
    def __init__(self) -> None:
        configure_logging("INFO")

        self.root = tk.Tk()
        self.root.title("Giuseppe Rubino - PDF to XLSX Converter")

        self.input_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.debug_var = tk.BooleanVar(value=False)
        self.ocr_var = tk.BooleanVar(value=False)
        self.status_var = tk.StringVar(value="Ready")
        self._queue: queue.Queue = queue.Queue()
        self._worker: Optional[threading.Thread] = None

        self._setup_style()
        self._build_ui()
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

        self._add_row(form, "Input PDF", self.input_var, self._browse_input)
        self._add_row(form, "Output XLSX", self.output_var, self._browse_output)

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

        button_frame = ttk.Frame(form)
        button_frame.pack(fill="x", pady=6)
        self.convert_button = ttk.Button(
            button_frame,
            text="Converti",
            command=self._run_job,
            style="Accent.TButton",
        )
        self.convert_button.pack(side="right")

        progress_frame = ttk.Frame(self.root, padding=(10, 0, 10, 6))
        progress_frame.pack(fill="x")
        self.progress = ttk.Progressbar(
            progress_frame,
            orient="horizontal",
            mode="determinate",
            style="Accent.Horizontal.TProgressbar",
        )
        self.progress.pack(fill="x")

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
        path = filedialog.askopenfilename(
            initialdir=config.INPUT_DIR,
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")]
        )
        if path:
            self.input_var.set(path)
            if not self.output_var.get().strip():
                output_dir = os.path.join(os.getcwd(), config.OUTPUT_DIR)
                os.makedirs(output_dir, exist_ok=True)
                stem = os.path.splitext(os.path.basename(path))[0]
                self.output_var.set(os.path.join(output_dir, f"{stem}.xlsx"))

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

        self.convert_button.configure(state="disabled")
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

    def _progress_callback(
        self, processed: int, total: int, page_number: int, pdf_total: int
    ) -> None:
        self._queue.put(
            ("progress", processed, total, page_number, pdf_total)
        )

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
                self.convert_button.configure(state="normal")
            elif kind == "error":
                message = item[1]
                self._log(f"Error: {message}")
                self.status_var.set("Error")
                self.convert_button.configure(state="normal")
                messagebox.showerror("Error", message)
        self.root.after(100, self._poll_queue)

    def start(self) -> None:
        self.root.mainloop()


def start() -> None:
    App().start()
