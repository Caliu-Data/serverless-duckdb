"""Main contract validator that orchestrates all validation steps."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import duckdb

from comboi.contracts.contract_loader import ContractLoader, DataContract
from comboi.contracts.quality_validator import QualityValidator, QualityValidationResult
from comboi.contracts.schema_validator import SchemaValidator, SchemaValidationResult
from comboi.contracts.sla_validator import SLAValidator, SLAValidationResult
from comboi.logging import get_logger

logger = get_logger(__name__)


class ContractValidationResult:
    """Complete result of contract validation."""

    def __init__(
        self,
        contract: DataContract,
        schema_result: SchemaValidationResult,
        quality_result: QualityValidationResult,
        sla_result: Optional[SLAValidationResult] = None,
    ):
        self.contract = contract
        self.schema_result = schema_result
        self.quality_result = quality_result
        self.sla_result = sla_result

    @property
    def passed(self) -> bool:
        """Check if all validations passed."""
        if not self.schema_result.passed:
            return False
        if not self.quality_result.passed:
            return False
        if self.sla_result and not self.sla_result.passed:
            return False
        return True

    @property
    def all_errors(self) -> List[str]:
        """Get all errors from all validations."""
        errors = []
        errors.extend(self.schema_result.errors)
        errors.extend(self.quality_result.errors)
        if self.sla_result:
            errors.extend(self.sla_result.errors)
        return errors

    @property
    def all_warnings(self) -> List[str]:
        """Get all warnings from all validations."""
        warnings = []
        warnings.extend(self.schema_result.warnings)
        warnings.extend(self.quality_result.warnings)
        if self.sla_result:
            warnings.extend(self.sla_result.warnings)
        return warnings

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        msg = f"Contract validation for {self.contract.dataset}: {status}"
        if self.all_errors:
            msg += f" ({len(self.all_errors)} errors)"
        if self.all_warnings:
            msg += f" ({len(self.all_warnings)} warnings)"
        return msg


class ContractValidator:
    """Main validator that orchestrates contract validation."""

    def __init__(self, contracts_path: Path):
        """
        Initialize contract validator.

        Args:
            contracts_path: Path to directory containing contract YAML files
        """
        self.contracts_path = Path(contracts_path)
        self.loader = ContractLoader(contracts_path)

    def validate(
        self,
        contract_name: str,
        data_path: Path,
        dataset_name: str,
        validate_sla: bool = True,
    ) -> ContractValidationResult:
        """
        Validate data against a contract.

        Args:
            contract_name: Name of the contract to validate against
            data_path: Path to the data file (Parquet)
            dataset_name: Name to use for the dataset view in DuckDB
            validate_sla: Whether to validate SLA requirements

        Returns:
            ContractValidationResult with all validation results

        Raises:
            FileNotFoundError: If contract or data file doesn't exist
            RuntimeError: If validation fails
        """
        # Load contract
        contract = self.loader.load(contract_name)

        # Create DuckDB connection
        con = duckdb.connect()
        try:
            # Load data into view
            con.execute(
                f"CREATE OR REPLACE VIEW {dataset_name} AS SELECT * FROM read_parquet('{data_path.as_posix()}')"
            )

            # Get row count for SLA validation
            row_count = con.execute(f"SELECT COUNT(*) FROM {dataset_name}").fetchone()[0]

            # Run schema validation
            schema_validator = SchemaValidator(contract)
            schema_result = schema_validator.validate(con, dataset_name)

            # Run quality validation
            quality_validator = QualityValidator(contract)
            quality_result = quality_validator.validate(con, dataset_name)

            # Run SLA validation (optional)
            sla_result = None
            if validate_sla:
                sla_validator = SLAValidator(contract)
                sla_result = sla_validator.validate(data_path, row_count)

            return ContractValidationResult(
                contract=contract,
                schema_result=schema_result,
                quality_result=quality_result,
                sla_result=sla_result,
            )

        finally:
            con.close()

    def validate_and_report(
        self,
        contract_name: str,
        data_path: Path,
        dataset_name: str,
        validate_sla: bool = True,
    ) -> bool:
        """
        Validate data against a contract and report results.

        Args:
            contract_name: Name of the contract to validate against
            data_path: Path to the data file (Parquet)
            dataset_name: Name to use for the dataset view in DuckDB
            validate_sla: Whether to validate SLA requirements

        Returns:
            True if validation passed, False otherwise

        Raises:
            RuntimeError: If validation fails with errors
        """
        result = self.validate(contract_name, data_path, dataset_name, validate_sla)

        # Report results
        logger.info(
            "Contract validation started",
            dataset=result.contract.dataset,
            version=result.contract.version,
        )

        # Schema results
        if result.schema_result.passed:
            logger.info("Schema validation passed", dataset=result.contract.dataset)
        else:
            logger.error(
                "Schema validation failed",
                dataset=result.contract.dataset,
                errors=result.schema_result.errors,
            )
        if result.schema_result.warnings:
            logger.warning(
                "Schema validation warnings",
                dataset=result.contract.dataset,
                warnings=result.schema_result.warnings,
            )

        # Quality results
        if result.quality_result.passed:
            logger.info("Quality validation passed", dataset=result.contract.dataset)
        else:
            logger.error(
                "Quality validation failed",
                dataset=result.contract.dataset,
                errors=result.quality_result.errors,
            )
        if result.quality_result.warnings:
            logger.warning(
                "Quality validation warnings",
                dataset=result.contract.dataset,
                warnings=result.quality_result.warnings,
            )

        # SLA results
        if result.sla_result:
            if result.sla_result.passed:
                logger.info("SLA validation passed", dataset=result.contract.dataset)
            else:
                logger.error(
                    "SLA validation failed",
                    dataset=result.contract.dataset,
                    errors=result.sla_result.errors,
                )
            if result.sla_result.warnings:
                logger.warning(
                    "SLA validation warnings",
                    dataset=result.contract.dataset,
                    warnings=result.sla_result.warnings,
                )

        # Overall result
        if result.passed:
            logger.info(
                "All contract validations passed",
                dataset=result.contract.dataset,
            )
            return True
        else:
            error_summary = f"Contract validation failed for {result.contract.dataset}: {len(result.all_errors)} errors"
            logger.error(
                "Contract validation failed",
                dataset=result.contract.dataset,
                error_count=len(result.all_errors),
                errors=result.all_errors,
            )
            raise RuntimeError(error_summary)

