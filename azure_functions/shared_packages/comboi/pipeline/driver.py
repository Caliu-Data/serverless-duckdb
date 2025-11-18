from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, List, Optional

from comboi.checkpoint import CheckpointStore
from comboi.config import PipelineConfig
from comboi.io.adls import ADLSClient
from comboi.pipeline.executor import Executor
from comboi.pipeline.monitoring import Monitor
from comboi.pipeline.stages.bronze import BronzeStage
from comboi.pipeline.stages.gold import GoldStage
from comboi.pipeline.stages.silver import SilverStage


class Driver:
    def __init__(
        self,
        config: PipelineConfig,
        checkpoint_store: CheckpointStore,
    ):
        self.config = config
        self.monitor = Monitor(
            log_path=config.monitoring.log_path,
            metrics_path=config.monitoring.metrics_path,
            azure_connection_string=config.monitoring.azure_connection_string,
        )
        bronze_conf = config.stages.bronze
        silver_conf = config.stages.silver
        gold_conf = config.stages.gold

        self.bronze_stage = BronzeStage(
            checkpoint_store=checkpoint_store,
            data_lake=ADLSClient(**bronze_conf["data_lake"]),
            local_landing=Path(bronze_conf["local_path"]),
        )
        # Set base paths for dbt variables
        bronze_base = str(Path(bronze_conf["local_path"]).resolve())
        silver_base = str(Path(silver_conf["local_path"]).resolve())
        silver_conf["bronze_base_path"] = bronze_base
        gold_conf["silver_base_path"] = silver_base
        
        self.silver_stage = SilverStage(
            data_lake=ADLSClient(**silver_conf["data_lake"]),
            local_silver=Path(silver_conf["local_path"]),
        )
        self.gold_stage = GoldStage(
            data_lake=ADLSClient(**gold_conf["data_lake"]),
            local_gold=Path(gold_conf["local_path"]),
        )
        self.executor = Executor(monitor=self.monitor)

    def run(self, selected: Optional[str] = None) -> Dict[str, str]:
        stages = self.execution_order(selected)
        task_map = self._task_map()
        active_tasks = {stage: task_map[stage] for stage in stages}
        return self.executor.run(stages, active_tasks)

    def plan(self, selected: Optional[str] = None) -> List[str]:
        return self.execution_order(selected)

    def execution_order(self, selected: Optional[str]) -> List[str]:
        order = ["bronze", "silver", "gold"]
        if not selected or selected == "all":
            return order
        stage = selected.lower()
        if stage not in order:
            raise ValueError(f"Unknown stage {selected}")
        idx = order.index(stage)
        return order[idx:]

    def run_stage(self, stage: str) -> str:
        task = self._task_map().get(stage)
        if task is None:
            raise ValueError(f"No task registered for stage '{stage}'")
        return task()

    def _task_map(self) -> Dict[str, Callable[[], str]]:
        sources_payload = [src.__dict__ for src in self.config.sources]
        return {
            "bronze": lambda: self._serialize(
                "bronze_outputs",
                self.bronze_stage.run(
                    sources_payload,
                    self.config.stages.bronze,
                ),
            ),
            "silver": lambda: self._serialize(
                "silver_outputs",
                self.silver_stage.run(self.config.stages.silver),
            ),
            "gold": lambda: self._serialize(
                "gold_outputs",
                self.gold_stage.run(self.config.stages.gold),
            ),
        }

    def _serialize(self, metric_key: str, outputs):
        joined = ",".join(outputs)
        self.monitor.record_metric(metric_key, outputs)
        return joined

