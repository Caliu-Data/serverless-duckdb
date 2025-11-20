"""Schema validator for data contracts."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import duckdb

from comboi.contracts.contract_loader import ColumnDefinition, DataContract


class SchemaValidationResult:
    """Result of schema validation."""

    def __init__(self, passed: bool, errors: List[str], warnings: List[str]):
        self.passed = passed
        self.errors = errors
        self.warnings = warnings

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        msg = f"Schema validation: {status}"
        if self.errors:
            msg += f" ({len(self.errors)} errors)"
        if self.warnings:
            msg += f" ({len(self.warnings)} warnings)"
        return msg


class SchemaValidator:
    """Validates data against schema definitions in contracts."""

    def __init__(self, contract: DataContract):
        """
        Initialize schema validator.

        Args:
            contract: Data contract to validate against
        """
        self.contract = contract

    def validate(self, con: duckdb.DuckDBPyConnection, dataset_name: str) -> SchemaValidationResult:
        """
        Validate dataset schema against contract.

        Args:
            con: DuckDB connection with dataset loaded
            dataset_name: Name of the dataset view

        Returns:
            SchemaValidationResult with validation results
        """
        errors: List[str] = []
        warnings: List[str] = []

        # Get actual schema from DuckDB
        try:
            actual_columns = con.execute(f"DESCRIBE {dataset_name}").fetchall()
        except Exception as e:
            return SchemaValidationResult(
                False, [f"Failed to describe dataset: {e}"], []
            )

        actual_column_map = {row[0]: {"type": row[1], "null": row[2], "key": row[3], "default": row[4], "extra": row[5]} for row in actual_columns}
        actual_column_names = set(actual_column_map.keys())

        # Validate columns exist
        expected_columns = {col.name for col in self.contract.columns}
        missing_columns = expected_columns - actual_column_names
        extra_columns = actual_column_names - expected_columns

        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        if extra_columns:
            warnings.append(f"Extra columns found (not in contract): {extra_columns}")

        # Validate each column
        for col_def in self.contract.columns:
            if col_def.name not in actual_column_map:
                continue  # Already reported as missing

            actual_col = actual_column_map[col_def.name]
            col_errors, col_warnings = self._validate_column(col_def, actual_col, con, dataset_name)
            errors.extend(col_errors)
            warnings.extend(col_warnings)

        return SchemaValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def _validate_column(
        self,
        col_def: ColumnDefinition,
        actual_col: Dict[str, Any],
        con: duckdb.DuckDBPyConnection,
        dataset_name: str,
    ) -> Tuple[List[str], List[str]]:
        """Validate a single column."""
        errors: List[str] = []
        warnings: List[str] = []

        # Check nullability
        if not col_def.nullable and actual_col["null"] == "YES":
            errors.append(f"Column {col_def.name} is nullable but contract requires NOT NULL")

        # Validate constraints
        if col_def.constraints:
            for constraint in col_def.constraints:
                if "not_null" in constraint and constraint["not_null"]:
                    null_count = con.execute(
                        f"SELECT COUNT(*) FROM {dataset_name} WHERE {col_def.name} IS NULL"
                    ).fetchone()[0]
                    if null_count > 0:
                        errors.append(
                            f"Column {col_def.name} has {null_count} NULL values but contract requires NOT NULL"
                        )

                if "unique" in constraint and constraint["unique"]:
                    duplicate_count = con.execute(
                        f"""
                        SELECT COUNT(*) FROM (
                            SELECT {col_def.name}, COUNT(*) as cnt
                            FROM {dataset_name}
                            GROUP BY {col_def.name}
                            HAVING COUNT(*) > 1
                        )
                        """
                    ).fetchone()[0]
                    if duplicate_count > 0:
                        errors.append(
                            f"Column {col_def.name} has duplicates but contract requires UNIQUE"
                        )

                if "min_value" in constraint:
                    min_val = constraint["min_value"]
                    violation_count = con.execute(
                        f"SELECT COUNT(*) FROM {dataset_name} WHERE {col_def.name} < {min_val}"
                    ).fetchone()[0]
                    if violation_count > 0:
                        errors.append(
                            f"Column {col_def.name} has {violation_count} values below minimum {min_val}"
                        )

                if "max_value" in constraint:
                    max_val = constraint["max_value"]
                    violation_count = con.execute(
                        f"SELECT COUNT(*) FROM {dataset_name} WHERE {col_def.name} > {max_val}"
                    ).fetchone()[0]
                    if violation_count > 0:
                        errors.append(
                            f"Column {col_def.name} has {violation_count} values above maximum {max_val}"
                        )

                if "allowed_values" in constraint:
                    allowed = constraint["allowed_values"]
                    # Check for values not in allowed list
                    placeholders = ",".join([f"'{v}'" for v in allowed])
                    violation_count = con.execute(
                        f"SELECT COUNT(*) FROM {dataset_name} WHERE {col_def.name} NOT IN ({placeholders}) AND {col_def.name} IS NOT NULL"
                    ).fetchone()[0]
                    if violation_count > 0:
                        errors.append(
                            f"Column {col_def.name} has {violation_count} values not in allowed list: {allowed}"
                        )

                if "pattern" in constraint:
                    # DuckDB doesn't have regex support in all versions, so we'll use LIKE patterns
                    # This is a simplified check
                    pattern = constraint["pattern"]
                    # For email pattern, we'll do a basic check
                    if "@" in pattern:  # Email pattern
                        violation_count = con.execute(
                            f"SELECT COUNT(*) FROM {dataset_name} WHERE {col_def.name} IS NOT NULL AND {col_def.name} NOT LIKE '%@%.%'"
                        ).fetchone()[0]
                        if violation_count > 0:
                            warnings.append(
                                f"Column {col_def.name} has {violation_count} values that may not match pattern {pattern}"
                            )

        return errors, warnings

