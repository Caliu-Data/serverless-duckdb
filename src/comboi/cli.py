from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from comboi.logging import get_logger
from comboi.pipeline.driver import Driver
from comboi.runner import create_driver

logger = get_logger(__name__)

app = typer.Typer(help="Serverless DuckDB Medallion ETL executor")


def _load_driver(config_path: Path) -> Driver:
    return create_driver(config_path)


@app.command("run")
def run_pipeline(
    stage: Optional[str] = typer.Argument(None, help="Stage to run: bronze|silver|gold|all"),
    config: Path = typer.Option(
        Path("configs/default.yml"),
        "--config",
        "-c",
        help="Path to pipeline configuration file",
    ),
) -> None:
    driver = _load_driver(config)
    results = driver.run(selected=stage)
    logger.info("Pipeline completed successfully", stage=stage or "all", results=results)
    for name, output in results.items():
        logger.info("Stage output", stage=name, output=output)


@app.command("plan")
def plan_pipeline(
    stage: Optional[str] = typer.Argument(None, help="Stage to plan"),
    config: Path = typer.Option(Path("configs/default.yml"), "--config", "-c"),
) -> None:
    driver = _load_driver(config)
    planned = driver.plan(stage)
    logger.info("Planned tasks", stage=stage or "all", tasks=planned)

