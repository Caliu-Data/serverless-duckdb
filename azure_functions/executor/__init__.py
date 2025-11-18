from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import azure.functions as func
from rich.console import Console

_FUNCTION_ROOT = Path(__file__).resolve().parents[1]
_SHARED = _FUNCTION_ROOT / "shared_packages"
if _SHARED.exists():
    sys.path.insert(0, str(_SHARED))

from comboi.pipeline.queue import AzureTaskQueue
from comboi.runner import create_driver

console = Console()


def main(msg: func.QueueMessage) -> None:
    payload = _parse_payload(msg)
    config_path = Path(payload.get("config_path", os.getenv("COMBOI_CONFIG", "configs/default.yml")))
    stage = payload["stage"]
    remaining: List[str] = payload.get("remaining", [])

    driver = create_driver(config_path)
    console.log(f"[blue]Executing stage {stage}[/]")
    result = driver.run_stage(stage)
    console.log(f"[green]Stage {stage} completed[/] -> {result}")

    if remaining:
        _enqueue_next(driver, config_path, remaining)


def _parse_payload(message: func.QueueMessage) -> Dict[str, Any]:
    try:
        return message.get_json()
    except ValueError as exc:
        raise ValueError("Expected JSON payload in queue message") from exc


def _enqueue_next(driver, config_path: Path, remaining: List[str]) -> None:
    queue_conf = driver.config.queue
    queue = AzureTaskQueue.from_connection_string(
        connection_string=queue_conf.connection_string,
        queue_name=queue_conf.queue_name,
        visibility_timeout=queue_conf.visibility_timeout or 300,
    )
    next_stage = remaining[0]
    payload = {
        "config_path": str(config_path),
        "stage": next_stage,
        "remaining": remaining[1:],
    }
    queue.enqueue(payload)
    console.log(f"[magenta]Queued next stage {next_stage}[/]")

