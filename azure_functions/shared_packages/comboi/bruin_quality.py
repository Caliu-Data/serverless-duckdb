from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict, List

import duckdb
from rich.console import Console

console = Console()


class QualityCheckResult:
    def __init__(self, name: str, passed: bool, message: str = ""):
        self.name = name
        self.passed = passed
        self.message = message

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        return f"{self.name}: {status}" + (f" - {self.message}" if self.message else "")


class BruinQualityRunner:
    def __init__(self, transformations_path: Path):
        self.transformations_path = transformations_path

    def run_quality_checks(
        self,
        quality_check_names: List[str],
        data_path: Path,
        dataset_name: str,
    ) -> None:
        """Run bruin quality check scripts and raise if any fail."""
        if not quality_check_names:
            return

        console.log(f"[cyan]Running bruin quality checks for {dataset_name}[/]")

        # Create DuckDB connection for quality checks
        con = duckdb.connect()
        try:
            # Load the data into a view
            con.execute(
                f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM read_parquet('{data_path.as_posix()}')"
            )

            all_results: List[QualityCheckResult] = []

            for check_name in quality_check_names:
                result = self._run_single_check(check_name, con, dataset_name)
                all_results.append(result)

            # Report results
            failed_checks = [r for r in all_results if not r.passed]
            if failed_checks:
                console.log("[red]Quality check failures:[/]")
                for result in failed_checks:
                    console.log(f"[red]  {result}[/]")
                raise RuntimeError(
                    f"Quality checks failed for {dataset_name}: {len(failed_checks)} of {len(all_results)} checks failed"
                )

            console.log(f"[green]All quality checks passed for {dataset_name}[/]")

        finally:
            con.close()

    def _run_single_check(
        self, check_name: str, con: duckdb.DuckDBPyConnection, dataset_name: str
    ) -> QualityCheckResult:
        """Run a single quality check script."""
        check_file = self.transformations_path / "quality" / f"{check_name}.py"
        if not check_file.exists():
            return QualityCheckResult(
                check_name, False, f"Quality check script not found: {check_file}"
            )

        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(check_name, check_file)
        if spec is None or spec.loader is None:
            return QualityCheckResult(
                check_name, False, f"Could not load quality check module: {check_name}"
            )

        module = importlib.util.module_from_spec(spec)
        sys.modules[check_name] = module
        spec.loader.exec_module(module)

        # Check if check function exists
        if not hasattr(module, "check"):
            return QualityCheckResult(
                check_name,
                False,
                f"Quality check {check_name} must define a 'check' function",
            )

        try:
            # Call the check function
            # The function should return a tuple (passed: bool, message: str) or just bool
            check_func = getattr(module, "check")
            result = check_func(con=con, dataset_name=dataset_name)

            # Handle different return types
            if isinstance(result, bool):
                return QualityCheckResult(check_name, result)
            elif isinstance(result, tuple) and len(result) == 2:
                passed, message = result
                return QualityCheckResult(check_name, bool(passed), str(message))
            else:
                return QualityCheckResult(
                    check_name,
                    False,
                    f"Check function must return bool or (bool, str), got {type(result)}",
                )

        except Exception as exc:
            return QualityCheckResult(
                check_name, False, f"Error running check: {exc}"
            )

