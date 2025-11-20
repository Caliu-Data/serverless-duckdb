# Data Contracts

This directory contains data contract definitions for datasets in the pipeline. Data contracts provide formal specifications for:

- **Schema**: Column names, types, nullability, and constraints
- **Quality Rules**: Business rules and data quality checks
- **SLAs**: Freshness and completeness requirements
- **Evolution Policy**: Rules for how contracts can change over time

## Contract Structure

Each contract is a YAML file with the following structure:

```yaml
version: "1.0.0"
dataset: "dataset_name"
stage: "silver"  # or "bronze", "gold"
owner: "team-name"
description: "Description of the dataset"

schema:
  columns:
    - name: column_name
      type: VARCHAR  # or INTEGER, DECIMAL, DATE, etc.
      nullable: false
      description: "Column description"
      constraints:
        - unique: true
        - not_null: true
        - min_value: 0
        - max_value: 100
        - allowed_values: ["value1", "value2"]
        - pattern: "^regex_pattern$"

quality_rules:
  - name: "rule_name"
    type: "uniqueness"  # or "not_null", "volume", "custom_sql"
    column: "column_name"  # required for uniqueness/not_null
    severity: "error"  # or "warning"
    query: "SELECT ..."  # for custom_sql
    expected: 0  # expected result for custom_sql
    min_rows: 1  # for volume checks

sla:
  freshness:
    max_age_hours: 24
    schedule: "daily"
  completeness:
    min_row_count: 1
    expected_growth_rate: 0.05

evolution:
  backward_compatible: true
  breaking_changes_allowed: false
  deprecation_notice_days: 30
```

## Quality Rule Types

### Uniqueness
Validates that a column has unique values:
```yaml
- name: "no_duplicates"
  type: "uniqueness"
  column: "id"
  severity: "error"
```

### Not Null
Validates that a column has no NULL values:
```yaml
- name: "no_null_ids"
  type: "not_null"
  column: "id"
  severity: "error"
```

### Volume
Validates minimum row count:
```yaml
- name: "minimum_rows"
  type: "volume"
  min_rows: 1
  severity: "error"
```

### Custom SQL
Validates using a custom SQL query:
```yaml
- name: "positive_totals"
  type: "custom_sql"
  query: "SELECT COUNT(*) FROM {dataset} WHERE total <= 0"
  expected: 0
  severity: "error"
```

## Usage

Contracts are automatically validated through the Bruin quality check system. Reference contracts in `configs/transformations.yml` using the `contract:` prefix in the `quality_checks` field:

```yaml
silver:
  - name: orders_clean
    inputs:
      - alias: bronze_orders
        stage: bronze
        source_path: "sales_azure/orders.parquet"
    quality_checks:
      - contract:orders_clean  # References contracts/orders_clean.yml
```

All quality checks (including contracts) are executed through the Bruin quality check framework. You can mix contract-based checks with traditional Python quality check scripts:

```yaml
quality_checks:
  - contract:orders_clean  # Contract-based validation
  - custom_check  # Traditional Python quality check script
```

## Validation Process

When a contract is specified, the pipeline will:

1. **Schema Validation**: Check that all required columns exist, have correct types, and meet constraints
2. **Quality Validation**: Execute all quality rules defined in the contract
3. **SLA Validation**: Check freshness (file age) and completeness (row count)

If any validation fails with severity "error", the pipeline will stop and raise an exception. Warnings are logged but don't stop execution.

## Contract Versioning

Contracts include a version field. When updating contracts:

- **Backward Compatible Changes**: Adding optional columns, relaxing constraints
- **Breaking Changes**: Removing columns, making nullable columns required, changing types

Set `breaking_changes_allowed: false` to prevent accidental breaking changes.

## Examples

See `orders_clean.yml` and `customers_clean.yml` for complete examples.

