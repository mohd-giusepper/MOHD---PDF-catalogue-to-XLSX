import json
from typing import Any


def write_debug_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)
