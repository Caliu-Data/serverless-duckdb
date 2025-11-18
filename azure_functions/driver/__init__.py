from __future__ import annotations

import os
import sys
from pathlib import Path

import azure.functions as func
from rich.console import Console

_FUNCTION_ROOT = Path(__file__).resolve().parents[1]
_SHARED = _FUNCTION_ROOT / "shared_packages"
if _SHARED.exists():
    sys.path.insert(0, str(_SHARED))

from comboi.pipeline.queue import AzureTaskQueue
from comboi.runner import create_driver

console = Console()


def main(mytimer: func.TimerRequest) -> None:
    config_path = Path(os.getenv("COMBOI_CONFIG", "configs/default.yml"))
    selected_stage = os.getenv("COMBOI_START_STAGE", "all")

    driver = create_driver(config_path)
    stages = driver.execution_order(selected_stage)
    if not stages:
        console.log("[yellow]No stages to enqueue[/]")
        return

    queue_conf = driver.config.queue
    queue = AzureTaskQueue.from_connection_string(
        connection_string=queue_conf.connection_string,
        queue_name=queue_conf.queue_name,
        visibility_timeout=queue_conf.visibility_timeout or 300,
    )

    payload = {
        "config_path": str(config_path),
        "stage": stages[0],
        "remaining": stages[1:],
    }
    queue.enqueue(payload)
    console.log(f"[green]Scheduled pipeline run starting with stage {stages[0]}[/]")

