from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from comboi.checkpoint import CheckpointStore
from comboi.connectors import AzureSQLConnector, PostgresConnector
from comboi.io.adls import ADLSClient
from comboi.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BronzeStage:
    checkpoint_store: CheckpointStore
    data_lake: ADLSClient
    local_landing: Path

    def run(self, sources: List[Dict], stage_conf: Dict) -> List[str]:
        outputs: List[str] = []
        for src in sources:
            connector = self._build_connector(src)
            for table in src["tables"]:
                bronze_path = self.local_landing / src["name"] / f"{table['name']}.parquet"
                bronze_path.parent.mkdir(parents=True, exist_ok=True)

                remote_path = stage_conf["remote_path_template"].format(
                    stage="bronze",
                    source=src["name"],
                    table=table["name"],
                )
                checkpoint_key = table.get("checkpoint_key", src.get("checkpoint_key"))

                exported = connector.export_table(
                    table, bronze_path, checkpoint_key=checkpoint_key
                )
                remote_uri = self.data_lake.upload(exported, remote_path)
                outputs.append(remote_uri)
        logger.info("Bronze stage completed", parquet_files=len(outputs))
        return outputs

    def _build_connector(self, src: Dict):
        if src["type"] == "azure_sql":
            return AzureSQLConnector(
                dsn=src["connection"]["dsn"],
                checkpoint_store=self.checkpoint_store,
            )
        if src["type"] == "postgres":
            return PostgresConnector(
                conn_str=src["connection"]["conn_str"],
                checkpoint_store=self.checkpoint_store,
            )
        raise ValueError(f"Unsupported source type {src['type']}")

