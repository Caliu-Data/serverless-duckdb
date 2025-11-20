from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import duckdb

from comboi.checkpoint import CheckpointStore
from comboi.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PostgresConnector:
    conn_str: str
    checkpoint_store: CheckpointStore

    def export_table(
        self,
        table_cfg: Dict[str, str],
        destination: Path,
        checkpoint_key: Optional[str] = None,
    ) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        query = table_cfg["query"]
        incremental_column = table_cfg.get("incremental_column")
        last_value = None

        if checkpoint_key and incremental_column:
            last_value = self.checkpoint_store.get(checkpoint_key)
            if last_value:
                query = f"""
                SELECT * FROM ({query}) AS src
                WHERE {incremental_column} > '{last_value}'
                """

        logger.info("Executing Postgres query", table=table_cfg["name"])
        con = duckdb.connect()
        try:
            con.execute("INSTALL postgres_scanner;")
            con.execute("LOAD postgres_scanner;")
            con.execute(f"ATTACH '{self.conn_str}' (TYPE POSTGRES, READ_ONLY=TRUE)")
            con.execute(f"COPY ({query}) TO '{destination.as_posix()}' (FORMAT PARQUET)")

            if checkpoint_key and incremental_column:
                max_query = f"SELECT MAX({incremental_column}) AS chk FROM ({table_cfg['query']}) src"
                if last_value:
                    max_query += f" WHERE {incremental_column} > '{last_value}'"
                result = con.execute(max_query).fetchone()
                if result and result[0]:
                    self.checkpoint_store.update(checkpoint_key, result[0])
        finally:
            con.close()

        logger.info("Exported to destination", destination=str(destination), table=table_cfg["name"])
        return destination

