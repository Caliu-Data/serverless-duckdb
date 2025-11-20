from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from comboi.secrets import KeyVaultConfig, SecretResolver


@dataclass
class SourceConfig:
    name: str
    type: str
    connection: Dict[str, Any]
    tables: List[Dict[str, Any]]
    checkpoint_key: Optional[str] = None


@dataclass
class StageConfig:
    bronze: Dict[str, Any]
    silver: Dict[str, Any]
    gold: Dict[str, Any]


@dataclass
class MonitoringConfig:
    log_path: Path
    metrics_path: Path


@dataclass
class QueueConfig:
    connection_string: str
    queue_name: str
    visibility_timeout: Optional[int] = None


@dataclass
class PipelineConfig:
    sources: List[SourceConfig]
    stages: StageConfig
    monitoring: MonitoringConfig
    queue: QueueConfig
    key_vault: Optional[KeyVaultConfig] = None


def load_config(path: Path, transformations_path: Optional[Path] = None) -> PipelineConfig:
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # Load transformations config if provided
    transformations: Dict[str, Any] = {}
    if transformations_path and transformations_path.exists():
        with transformations_path.open("r", encoding="utf-8") as f:
            transformations = yaml.safe_load(f) or {}

    raw_key_vault = raw.get("key_vault")
    key_vault_cfg: Optional[KeyVaultConfig] = None
    if raw_key_vault:
        key_vault_cfg = KeyVaultConfig(vault_url=raw_key_vault["vault_url"])
        resolver = SecretResolver(key_vault_cfg)
        raw = resolver.resolve_structure(raw, skip_keys=("key_vault",))

    # Merge transformations into stages config
    if transformations:
        if "silver" in transformations:
            raw.setdefault("stages", {}).setdefault("silver", {})["transformations"] = transformations["silver"]
        if "gold" in transformations:
            raw.setdefault("stages", {}).setdefault("gold", {})["transformations"] = transformations["gold"]

    sources = [
        SourceConfig(
            name=src["name"],
            type=src["type"],
            connection=src["connection"],
            tables=src.get("tables", []),
            checkpoint_key=src.get("checkpoint_key"),
        )
        for src in raw["sources"]
    ]

    monitoring = MonitoringConfig(
        log_path=Path(raw["monitoring"]["log_path"]),
        metrics_path=Path(raw["monitoring"]["metrics_path"]),
    )

    queue_cfg = QueueConfig(
        connection_string=raw["queue"]["connection_string"],
        queue_name=raw["queue"]["queue_name"],
        visibility_timeout=raw["queue"].get("visibility_timeout"),
    )

    return PipelineConfig(
        sources=sources,
        stages=StageConfig(
            bronze=raw["stages"]["bronze"],
            silver=raw["stages"]["silver"],
            gold=raw["stages"]["gold"],
        ),
        monitoring=monitoring,
        queue=queue_cfg,
        key_vault=key_vault_cfg,
    )

