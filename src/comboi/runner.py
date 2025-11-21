from __future__ import annotations

import os
from pathlib import Path

from comboi.checkpoint import CheckpointStore
from comboi.config import load_config
from comboi.pipeline.driver import Driver


def create_driver(config_path: Path, transformations_path: Path | None = None) -> Driver:
    resolved_config = _resolve_path(config_path)
    # Try to find transformations.yml in the same directory as the config
    if transformations_path is None:
        transformations_candidate = resolved_config.parent / "transformations.yml"
        if transformations_candidate.exists():
            transformations_path = transformations_candidate
    config = load_config(resolved_config, transformations_path)
    _normalize_paths(config, resolved_config.parent)
    checkpoint_path = Path(config.stages.bronze["checkpoint_path"])
    checkpoint_store = CheckpointStore(_resolve_relative(checkpoint_path, resolved_config.parent))
    return Driver(config=config, checkpoint_store=checkpoint_store)


def _resolve_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    root = Path(__file__).resolve().parents[2]
    candidate = (root / path).resolve()
    if candidate.exists():
        return candidate
    return (Path.cwd() / path).resolve()


def _resolve_relative(path: Path, base: Path) -> Path:
    if path.is_absolute():
        return path
    return (base / path).resolve()


def _normalize_paths(config, base: Path) -> None:
    config.monitoring.log_path = _resolve_relative(Path(config.monitoring.log_path), base)
    config.monitoring.metrics_path = _resolve_relative(Path(config.monitoring.metrics_path), base)

    bronze = config.stages.bronze
    bronze["local_path"] = str(_resolve_relative(Path(bronze["local_path"]), base))
    if "checkpoint_path" in bronze:
        bronze["checkpoint_path"] = str(_resolve_relative(Path(bronze["checkpoint_path"]), base))

    silver = config.stages.silver
    silver["local_path"] = str(_resolve_relative(Path(silver["local_path"]), base))

    gold = config.stages.gold
    gold["local_path"] = str(_resolve_relative(Path(gold["local_path"]), base))

