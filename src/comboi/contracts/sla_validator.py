"""SLA validator for data contracts."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from comboi.contracts.contract_loader import DataContract, SLA


class SLAValidationResult:
    """Result of SLA validation."""

    def __init__(self, passed: bool, errors: List[str], warnings: List[str]):
        self.passed = passed
        self.errors = errors
        self.warnings = warnings

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        msg = f"SLA validation: {status}"
        if self.errors:
            msg += f" ({len(self.errors)} errors)"
        if self.warnings:
            msg += f" ({len(self.warnings)} warnings)"
        return msg


class SLAValidator:
    """Validates data against SLA definitions in contracts."""

    def __init__(self, contract: DataContract):
        """
        Initialize SLA validator.

        Args:
            contract: Data contract to validate against
        """
        self.contract = contract

    def validate(
        self,
        data_path: Path,
        row_count: int,
    ) -> SLAValidationResult:
        """
        Validate dataset against SLA requirements.

        Args:
            data_path: Path to the data file
            row_count: Number of rows in the dataset

        Returns:
            SLAValidationResult with validation results
        """
        errors: List[str] = []
        warnings: List[str] = []

        sla = self.contract.sla_object

        # Validate freshness
        if sla.freshness:
            freshness_errors, freshness_warnings = self._validate_freshness(data_path, sla.freshness)
            errors.extend(freshness_errors)
            warnings.extend(freshness_warnings)

        # Validate completeness
        if sla.completeness:
            completeness_errors, completeness_warnings = self._validate_completeness(row_count, sla.completeness)
            errors.extend(completeness_errors)
            warnings.extend(completeness_warnings)

        return SLAValidationResult(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def _validate_freshness(
        self, data_path: Path, freshness_config: dict
    ) -> tuple[List[str], List[str]]:
        """Validate data freshness."""
        errors: List[str] = []
        warnings: List[str] = []

        if "max_age_hours" in freshness_config:
            max_age_hours = freshness_config["max_age_hours"]
            if data_path.exists():
                file_mtime = datetime.fromtimestamp(data_path.stat().st_mtime)
                age_hours = (datetime.now() - file_mtime).total_seconds() / 3600
                if age_hours > max_age_hours:
                    errors.append(
                        f"Data freshness violation: File is {age_hours:.1f} hours old, max allowed is {max_age_hours} hours"
                    )
            else:
                errors.append(f"Data file not found: {data_path}")

        return errors, warnings

    def _validate_completeness(
        self, row_count: int, completeness_config: dict
    ) -> tuple[List[str], List[str]]:
        """Validate data completeness."""
        errors: List[str] = []
        warnings: List[str] = []

        if "min_row_count" in completeness_config:
            min_rows = completeness_config["min_row_count"]
            if row_count < min_rows:
                errors.append(
                    f"Completeness violation: Row count {row_count} is below minimum {min_rows}"
                )

        if "expected_growth_rate" in completeness_config:
            # This would require historical data to compare, so we'll just log a warning
            warnings.append(
                "Expected growth rate check requires historical data comparison (not implemented)"
            )

        return errors, warnings

