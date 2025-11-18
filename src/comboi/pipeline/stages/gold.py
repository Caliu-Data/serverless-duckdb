from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from rich.console import Console

from comboi.bruin_runner import BruinRunner
from comboi.io.adls import ADLSClient

console = Console()


@dataclass
class GoldStage:
    data_lake: ADLSClient
    local_gold: Path

    def run(self, stage_conf: Dict) -> List[str]:
        outputs: List[str] = []
        
        # Get bruin configuration
        transformations_path = Path(stage_conf.get("transformations_path", "transformations"))
        silver_base_path = stage_conf.get("silver_base_path", "data/silver")
        
        # Get transformations config
        transformations = stage_conf.get("transformations", {})
        gold_transforms = transformations.get("gold", [])
        
        if not gold_transforms:
            console.log("[yellow]No bruin transformations configured for Gold stage[/]")
            return outputs

        # Initialize bruin runner
        bruin_runner = BruinRunner(transformations_path=transformations_path)

        # Run bruin transformations
        input_base_paths = {"silver": silver_base_path}
        bruin_outputs = bruin_runner.run_transformations(
            "gold",
            gold_transforms,
            self.local_gold,
            input_base_paths,
        )

        # Upload each transformation output to ADLS
        for trans_config, bruin_output in zip(gold_transforms, bruin_outputs):
            trans_name = trans_config["name"]
            console.log(f"[bold blue]Uploading Gold transformation {trans_name}[/]")

            remote_path = stage_conf["remote_path_template"].format(
                stage="gold",
                source="metrics",
                table=trans_name,
            )
            remote_uri = self.data_lake.upload(bruin_output, remote_path)
            outputs.append(remote_uri)

        console.log(f"[bold green]Gold stage produced {len(outputs)} metrics[/]")
        return outputs

