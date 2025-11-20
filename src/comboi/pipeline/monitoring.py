from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

from comboi.logging import get_logger
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

logger = get_logger(__name__)


@dataclass
class Monitor:
    log_path: Path
    metrics_path: Path
    service_name: str = "comboi"
    _metrics: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, message: str, **kwargs: Any) -> None:
        """Log a message using structlog."""
        logger.info(message, service=self.service_name, **kwargs)
        # Also write to file for backward compatibility
        with self.log_path.open("a", encoding="utf-8") as fh:
            log_entry = {
                "message": message,
                "service": self.service_name,
                **kwargs,
            }
            fh.write(json.dumps(log_entry) + "\n")

    def record_metric(self, key: str, value: Any) -> None:
        """Record a metric."""
        self._metrics[key] = value
        with self.metrics_path.open("w", encoding="utf-8") as fh:
            json.dump(self._metrics, fh, indent=2)
        logger.info("metric_recorded", metric_key=key, metric_value=value, service=self.service_name)

    def progress(self) -> Progress:
        """Create a Rich progress bar."""
        # Use structlog logger for console output
        console_logger = structlog.get_logger()
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=None,  # Don't use Rich console, use structlog
        )
