from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional
from contextlib import contextmanager
from threading import Lock

_checkpoint_lock = Lock()


class CheckpointStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({})

    def _read(self) -> Dict[str, Any]:
        with self.path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def _write(self, data: Dict[str, Any]) -> None:
        tmp = self.path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
        tmp.replace(self.path)

    @contextmanager
    def session(self):
        with _checkpoint_lock:
            data = self._read()
            yield data
            self._write(data)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        with self.session() as data:
            return data.get(key, default)

    def update(self, key: str, value: Any) -> None:
        with self.session() as data:
            data[key] = value

