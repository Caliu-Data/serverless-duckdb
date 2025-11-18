from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich import print

from comboi.pipeline.driver import Driver
from comboi.runner import create_driver

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
    print("[bold green]Pipeline completed successfully[/]")
    for name, output in results.items():
        print(f"[white]{name}: {output}[/]")


@app.command("plan")
def plan_pipeline(
    stage: Optional[str] = typer.Argument(None, help="Stage to plan"),
    config: Path = typer.Option(Path("configs/default.yml"), "--config", "-c"),
) -> None:
    driver = _load_driver(config)
    planned = driver.plan(stage)
    print("[bold]Planned tasks:[/]")
    for item in planned:
        print(f"- {item}")

