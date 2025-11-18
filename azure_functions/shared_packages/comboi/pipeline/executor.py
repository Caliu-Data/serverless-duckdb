from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Iterable

from rich.console import Console

from comboi.pipeline.monitoring import Monitor

console = Console()


@dataclass
class Executor:
    monitor: Monitor

    def run(self, stages: Iterable[str], task_map: Dict[str, Callable[[], str]]) -> Dict[str, str]:
        results: Dict[str, str] = {}
        with self.monitor.progress() as progress:
            overall = progress.add_task("Executing pipeline", total=None)
            for name in stages:
                progress.log(f"Starting task: {name}")
                stage_task = progress.add_task(name, total=None)
                task = task_map.get(name)
                try:
                    if task is None:
                        raise KeyError(f"No task registered for stage '{name}'")
                    result = task()
                    results[name] = result
                    self.monitor.log(f"Task {name} completed: {result}")
                except Exception as exc:  # noqa: BLE001
                    self.monitor.log(f"Task {name} failed: {exc}")
                    raise
                finally:
                    progress.remove_task(stage_task)
            progress.remove_task(overall)
        return results
