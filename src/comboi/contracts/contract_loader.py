"""Contract loader for parsing and loading data contract definitions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class ColumnDefinition:
    """Definition of a column in a data contract."""

    name: str
    type: str
    nullable: bool
    description: Optional[str] = None
    constraints: Optional[List[Dict[str, Any]]] = None


@dataclass
class QualityRule:
    """Quality rule definition."""

    name: str
    type: str
    severity: str
    column: Optional[str] = None
    query: Optional[str] = None
    expected: Optional[Any] = None
    min_rows: Optional[int] = None


@dataclass
class SLA:
    """Service Level Agreement definition."""

    freshness: Optional[Dict[str, Any]] = None
    completeness: Optional[Dict[str, Any]] = None


@dataclass
class EvolutionPolicy:
    """Evolution policy for contract changes."""

    backward_compatible: bool = True
    breaking_changes_allowed: bool = False
    deprecation_notice_days: Optional[int] = None


@dataclass
class DataContract:
    """Complete data contract definition."""

    version: str
    dataset: str
    stage: str
    owner: str
    description: str
    schema: Dict[str, Any]
    quality_rules: List[Dict[str, Any]]
    sla: Dict[str, Any]
    evolution: Dict[str, Any]

    @property
    def columns(self) -> List[ColumnDefinition]:
        """Get column definitions."""
        columns = []
        for col_def in self.schema.get("columns", []):
            columns.append(
                ColumnDefinition(
                    name=col_def["name"],
                    type=col_def["type"],
                    nullable=col_def.get("nullable", True),
                    description=col_def.get("description"),
                    constraints=col_def.get("constraints", []),
                )
            )
        return columns

    @property
    def quality_rule_objects(self) -> List[QualityRule]:
        """Get quality rule objects."""
        rules = []
        for rule_def in self.quality_rules:
            rules.append(
                QualityRule(
                    name=rule_def["name"],
                    type=rule_def["type"],
                    severity=rule_def.get("severity", "error"),
                    column=rule_def.get("column"),
                    query=rule_def.get("query"),
                    expected=rule_def.get("expected"),
                    min_rows=rule_def.get("min_rows"),
                )
            )
        return rules

    @property
    def sla_object(self) -> SLA:
        """Get SLA object."""
        return SLA(
            freshness=self.sla.get("freshness"),
            completeness=self.sla.get("completeness"),
        )

    @property
    def evolution_policy(self) -> EvolutionPolicy:
        """Get evolution policy."""
        return EvolutionPolicy(
            backward_compatible=self.evolution.get("backward_compatible", True),
            breaking_changes_allowed=self.evolution.get("breaking_changes_allowed", False),
            deprecation_notice_days=self.evolution.get("deprecation_notice_days"),
        )


class ContractLoader:
    """Loads and parses data contract YAML files."""

    def __init__(self, contracts_path: Path):
        """
        Initialize contract loader.

        Args:
            contracts_path: Path to directory containing contract YAML files
        """
        self.contracts_path = Path(contracts_path)

    def load(self, contract_name: str) -> DataContract:
        """
        Load a contract by name.

        Args:
            contract_name: Name of the contract (without .yml extension)

        Returns:
            DataContract object

        Raises:
            FileNotFoundError: If contract file doesn't exist
            ValueError: If contract is invalid
        """
        contract_file = self.contracts_path / f"{contract_name}.yml"
        if not contract_file.exists():
            raise FileNotFoundError(f"Contract file not found: {contract_file}")

        with contract_file.open("r", encoding="utf-8") as f:
            contract_data = yaml.safe_load(f)

        if not contract_data:
            raise ValueError(f"Contract file is empty: {contract_file}")

        # Validate required fields
        required_fields = ["version", "dataset", "stage", "owner", "description", "schema", "quality_rules", "sla", "evolution"]
        missing_fields = [field for field in required_fields if field not in contract_data]
        if missing_fields:
            raise ValueError(f"Contract missing required fields: {missing_fields}")

        return DataContract(
            version=contract_data["version"],
            dataset=contract_data["dataset"],
            stage=contract_data["stage"],
            owner=contract_data["owner"],
            description=contract_data["description"],
            schema=contract_data["schema"],
            quality_rules=contract_data.get("quality_rules", []),
            sla=contract_data.get("sla", {}),
            evolution=contract_data.get("evolution", {}),
        )

    def list_contracts(self) -> List[str]:
        """
        List all available contracts.

        Returns:
            List of contract names (without .yml extension)
        """
        if not self.contracts_path.exists():
            return []
        return [f.stem for f in self.contracts_path.glob("*.yml")]

