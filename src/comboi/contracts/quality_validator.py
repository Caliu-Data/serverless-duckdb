"""Quality rule validator for data contracts."""

from __future__ import annotations

from typing import List

import duckdb

from comboi.contracts.contract_loader import DataContract, QualityRule


class QualityValidationResult:
    """Result of quality validation."""

    def __init__(self, passed: bool, errors: List[str], warnings: List[str]):
        self.passed = passed
        self.errors = errors
        self.warnings = warnings

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        msg = f"Quality validation: {status}"
        if self.errors:
            msg += f" ({len(self.errors)} errors)"
        if self.warnings:
            msg += f" ({len(self.warnings)} warnings)"
        return msg


class QualityValidator:
    """Validates data against quality rules in contracts."""

    def __init__(self, contract: DataContract):
        """
        Initialize quality validator.

        Args:
            contract: Data contract to validate against
        """
        self.contract = contract

    def validate(self, con: duckdb.DuckDBPyConnection, dataset_name: str) -> QualityValidationResult:
        """
        Validate dataset against quality rules.

        Args:
            con: DuckDB connection with dataset loaded
            dataset_name: Name of the dataset view

        Returns:
            QualityValidationResult with validation results
        """
        errors: List[str] = []
        warnings: List[str] = []

        for rule in self.contract.quality_rule_objects:
            rule_errors, rule_warnings = self._validate_rule(rule, con, dataset_name)
            if rule.severity == "error":
                errors.extend(rule_errors)
                warnings.extend(rule_warnings)
            else:
                warnings.extend(rule_errors)
                warnings.extend(rule_warnings)

        return QualityValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def _validate_rule(
        self, rule: QualityRule, con: duckdb.DuckDBPyConnection, dataset_name: str
    ) -> tuple[List[str], List[str]]:
        """Validate a single quality rule."""
        errors: List[str] = []
        warnings: List[str] = []

        try:
            if rule.type == "uniqueness":
                if not rule.column:
                    errors.append(f"Rule {rule.name}: uniqueness rule requires column")
                else:
                    duplicate_count = con.execute(
                        f"""
                        SELECT COUNT(*) FROM (
                            SELECT {rule.column}, COUNT(*) as cnt
                            FROM {dataset_name}
                            GROUP BY {rule.column}
                            HAVING COUNT(*) > 1
                        )
                        """
                    ).fetchone()[0]
                    if duplicate_count > 0:
                        msg = f"Rule {rule.name}: Found {duplicate_count} duplicate values in column {rule.column}"
                        if rule.severity == "error":
                            errors.append(msg)
                        else:
                            warnings.append(msg)

            elif rule.type == "not_null":
                if not rule.column:
                    errors.append(f"Rule {rule.name}: not_null rule requires column")
                else:
                    null_count = con.execute(
                        f"SELECT COUNT(*) FROM {dataset_name} WHERE {rule.column} IS NULL"
                    ).fetchone()[0]
                    if null_count > 0:
                        msg = f"Rule {rule.name}: Found {null_count} NULL values in column {rule.column}"
                        if rule.severity == "error":
                            errors.append(msg)
                        else:
                            warnings.append(msg)

            elif rule.type == "volume":
                row_count = con.execute(f"SELECT COUNT(*) FROM {dataset_name}").fetchone()[0]
                if rule.min_rows is not None and row_count < rule.min_rows:
                    msg = f"Rule {rule.name}: Row count {row_count} is below minimum {rule.min_rows}"
                    if rule.severity == "error":
                        errors.append(msg)
                    else:
                        warnings.append(msg)

            elif rule.type == "custom_sql":
                if not rule.query:
                    errors.append(f"Rule {rule.name}: custom_sql rule requires query")
                else:
                    # Replace {dataset} placeholder with actual dataset name
                    query = rule.query.format(dataset=dataset_name)
                    result = con.execute(query).fetchone()[0]
                    if rule.expected is not None and result != rule.expected:
                        msg = f"Rule {rule.name}: Query returned {result}, expected {rule.expected}"
                        if rule.severity == "error":
                            errors.append(msg)
                        else:
                            warnings.append(msg)

            else:
                warnings.append(f"Rule {rule.name}: Unknown rule type {rule.type}")

        except Exception as e:
            errors.append(f"Rule {rule.name}: Error executing validation: {e}")

        return errors, warnings

