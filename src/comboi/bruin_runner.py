from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict, List

import duckdb
from rich.console import Console

console = Console()


class BruinRunner:
    def __init__(self, transformations_path: Path):
        self.transformations_path = transformations_path

    def run_transformation(
        self,
        transformation_name: str,
        inputs: Dict[str, str],
        output_path: Path,
    ) -> Path:
        """Run a bruin transformation script and return the output parquet path."""
        console.log(f"[bold blue]Running bruin transformation: {transformation_name}[/]")

        # Load the transformation module
        transformation_file = self.transformations_path / f"{transformation_name}.py"
        if not transformation_file.exists():
            raise FileNotFoundError(
                f"Transformation script not found: {transformation_file}"
            )

        # Load the module dynamically
        spec = importlib.util.spec_from_file_location(
            transformation_name, transformation_file
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load transformation module: {transformation_name}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[transformation_name] = module
        spec.loader.exec_module(module)

        # Check if transform function exists
        if not hasattr(module, "transform"):
            raise AttributeError(
                f"Transformation {transformation_name} must define a 'transform' function"
            )

        # Create DuckDB connection for the transformation
        con = duckdb.connect()
        try:
            # Load input data into DuckDB views
            for alias, uri in inputs.items():
                con.execute(
                    f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM read_parquet('{uri}')"
                )

            # Call the transform function
            # The function should return a DuckDB query string or a pandas DataFrame
            transform_func = getattr(module, "transform")
            result = transform_func(con=con, inputs=inputs)

            # Handle different return types
            if isinstance(result, str):
                # SQL query string
                con.execute(
                    f"COPY ({result}) TO '{output_path.as_posix()}' (FORMAT PARQUET)"
                )
            elif hasattr(result, "to_parquet"):
                # pandas DataFrame
                output_path.parent.mkdir(parents=True, exist_ok=True)
                result.to_parquet(output_path)
            else:
                raise ValueError(
                    f"Transform function must return a SQL query string or pandas DataFrame, got {type(result)}"
                )

            console.log(f"[green]Transformation {transformation_name} completed[/]")
            return output_path

        finally:
            con.close()

    def run_transformations(
        self,
        stage: str,
        transformations: List[Dict[str, Any]],
        output_dir: Path,
        input_base_paths: Dict[str, str],
    ) -> List[Path]:
        """Run multiple bruin transformations and return output paths."""
        output_paths: List[Path] = []

        for trans_config in transformations:
            trans_name = trans_config["name"]
            console.log(f"[bold blue]Processing {stage} transformation: {trans_name}[/]")

            # Resolve input URIs
            inputs: Dict[str, str] = {}
            for input_config in trans_config.get("inputs", []):
                alias = input_config["alias"]
                source_path = input_config.get("source_path", "")
                stage_name = input_config.get("stage", "bronze")

                # Resolve full path
                if stage_name in input_base_paths:
                    base_path = input_base_paths[stage_name]
                    full_path = str(Path(base_path) / source_path)
                else:
                    full_path = source_path

                inputs[alias] = full_path

            # Determine output path
            output_path = output_dir / f"{trans_name}.parquet"

            # Run the transformation
            result_path = self.run_transformation(trans_name, inputs, output_path)
            output_paths.append(result_path)

        return output_paths

