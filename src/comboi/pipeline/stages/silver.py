from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import duckdb
from rich.console import Console
from splink.duckdb.duckdb_linker import DuckDBLinker

from comboi.bruin_runner import BruinRunner
from comboi.bruin_quality import BruinQualityRunner
from comboi.io.adls import ADLSClient

console = Console()


@dataclass
class SilverStage:
    data_lake: ADLSClient
    local_silver: Path

    def run(self, stage_conf: Dict) -> List[str]:
        outputs: List[str] = []
        
        # Get bruin configuration
        transformations_path = Path(stage_conf.get("transformations_path", "transformations"))
        bronze_base_path = stage_conf.get("bronze_base_path", "data/bronze")
        
        # Get transformations config
        transformations = stage_conf.get("transformations", {})
        silver_transforms = transformations.get("silver", [])
        
        if not silver_transforms:
            console.log("[yellow]No bruin transformations configured for Silver stage[/]")
            return outputs

        # Initialize bruin runner
        bruin_runner = BruinRunner(transformations_path=transformations_path)

        # Run bruin transformations
        input_base_paths = {"bronze": bronze_base_path}
        bruin_outputs = bruin_runner.run_transformations(
            "silver",
            silver_transforms,
            self.local_silver,
            input_base_paths,
        )

        # Process each transformation output
        for trans_config, bruin_output in zip(silver_transforms, bruin_outputs):
            trans_name = trans_config["name"]
            console.log(f"[bold blue]Processing Silver transformation {trans_name}[/]")

            # Run bruin quality checks if configured
            if "quality_checks" in trans_config:
                quality_runner = BruinQualityRunner(
                    transformations_path=transformations_path
                )
                quality_runner.run_quality_checks(
                    trans_config["quality_checks"],
                    bruin_output,
                    trans_name,
                )

            # Run Splink deduplication if configured
            if "splink" in trans_config:
                self._run_splink(trans_config, bruin_output)

            # Upload to ADLS
            remote_path = stage_conf["remote_path_template"].format(
                stage="silver", source="refined", table=trans_name
            )
            remote_uri = self.data_lake.upload(bruin_output, remote_path)
            outputs.append(remote_uri)

        console.log(f"[bold green]Silver stage produced {len(outputs)} datasets[/]")
        return outputs

    def _run_splink(self, trans_config: Dict, local_path: Path) -> None:
        splink_cfg = trans_config.get("splink")
        if not splink_cfg:
            return
        trans_name = trans_config["name"]
        console.log(f"[cyan]Running Splink deduplication for {trans_name}[/]")
        linker = DuckDBLinker(
            input_table_or_tables=[
                {
                    "table_name": trans_name,
                    "sql": f"SELECT * FROM read_parquet('{local_path.as_posix()}')",
                }
            ],
            settings_dict=splink_cfg,
        )
        splink_df = linker.deduplicate_table(
            trans_name,
            blocking_rule=splink_cfg.get("blocking_rule"),
            retain_matching_columns=True,
        )
        tmp_path = local_path.with_suffix(".dedup.parquet")
        linker.duckdb_connection().execute(
            f"COPY (SELECT * FROM {splink_df.physical_name}) TO '{tmp_path.as_posix()}' (FORMAT PARQUET)"
        )
        tmp_path.replace(local_path)

