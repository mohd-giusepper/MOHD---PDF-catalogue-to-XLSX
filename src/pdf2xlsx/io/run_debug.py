import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pdf2xlsx import config
from pdf2xlsx.io import debug_output


@dataclass
class RunDebugCollector:
    run_id: str
    started_at: str
    input_root: str
    run_type: str
    version: str
    _items_by_pdf: Dict[str, dict] = field(default_factory=dict, init=False, repr=False)

    @classmethod
    def start(cls, input_root: str = "", run_type: str = "run") -> "RunDebugCollector":
        now = datetime.now(timezone.utc)
        stamp = now.strftime("%Y%m%d_%H%M%S")
        run_id = f"{stamp}_{uuid.uuid4().hex[:6]}"
        return cls(
            run_id=run_id,
            started_at=now.isoformat(),
            input_root=input_root,
            run_type=run_type,
            version=config.APP_VERSION,
        )

    def add_pdf(
        self,
        pdf_path: str,
        triage_result,
        cached_pages,
        report: Optional[object] = None,
        reason: str = "",
    ) -> None:
        payload = debug_output.build_debug_payload(
            pdf_path=pdf_path,
            triage_result=triage_result,
            cached_pages=cached_pages,
            report=report,
            reason=reason,
        )
        self._items_by_pdf[pdf_path] = payload

    def write(self, output_dir: str) -> str:
        output_path = Path(output_dir) / f"{self.run_id}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pdf_items = list(self._items_by_pdf.values())
        pdf_items.sort(key=lambda item: item.get("summary", {}).get("pdf_path", ""))
        payload = {
            "run": {
                "run_id": self.run_id,
                "started_at": self.started_at,
                "run_type": self.run_type,
                "input_root": self.input_root,
                "version": self.version,
                "pdf_count": len(self._items_by_pdf),
            },
            "pdfs": pdf_items,
        }
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=True, indent=2)
        return str(output_path)
