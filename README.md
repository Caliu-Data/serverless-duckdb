# Comboi - Serverless DuckDB ELT System in Azure

[![Repository](https://img.shields.io/badge/GitHub-Caliu--Data%2Fcomboi-blue)](https://github.com/Caliu-Data/comboi)

This repository contains `comboi`, a Python-based ELT system that implements a medallion architecture on top of DuckDB, Azure Data Lake Storage using Azure Functions. It is designed to be configuration-driven and easy to operate.

## Key Features

- **Bronze landing**: Incremental extracts from Azure SQL Database and PostgreSQL using DuckDB, persisted as Parquet in Azure Data Lake Storage (ADLS) with checkpointing to avoid full reloads.
- **Silver refinement**: Data cleansing with [Bruin](https://github.com/bruin-data/bruin) quality checks and Splink-based deduplication, materialized back to ADLS.
- **Gold metrics**: Aggregations and business-ready metrics generated in DuckDB and delivered to ADLS.
- **Operational primitives**: Driver, Azure Storage Queue-backed queuer, executor, and monitoring primitives co-ordinate the full pipeline, with Azure Functions timer/queue triggers for serverless execution.
- **Secret management with Azure Key Vault**: Database passwords, storage account keys, queue connection strings, and Azure Monitor connection strings are fetched at runtime, keeping configurations free of hardcoded secrets.
- **Infrastructure-as-code**: The complete Azure footprint (Function App, Key Vault, Application Insights, Storage accounts, Data Lake containers, queues) is provisioned via Terraform.

## Repository Layout

```
.
├── configs/               # Pipeline configuration files
├── transformations/       # [Bruin](https://github.com/bruin-data/bruin) transformation and quality check scripts
├── src/comboi/         # Application source code (shared by CLI & Azure Functions)
│   ├── connectors/        # Source system connectors (Azure SQL, PostgreSQL)
│   ├── io/                # ADLS client helpers
│   ├── pipeline/          # Driver, queue, monitoring, and stage orchestration
│   └── cli.py             # Typer CLI entry point (optional local runs)
├── azure_functions/       # Timer and queue triggered Azure Functions entrypoints
└── terraform/             # Terraform IaC for all Azure resources
```

## Medallion Architecture

### Bronze: Landing Zone

1. DuckDB attaches to Azure SQL Database or PostgreSQL via their respective extensions (`odbc` and `postgres_scanner`).
2. Parameterized SQL queries defined in the configuration extract only new or changed records by leveraging checkpoint values.
3. Extracted data is written as Parquet to a local staging path and then uploaded to ADLS using `adlfs`.

### Silver: Curation Zone

1. Bronze Parquet assets are read via DuckDB.
2. [Bruin](https://github.com/bruin-data/bruin) transformation scripts (Python functions) apply SQL transformations to materialize curated views.
3. [Bruin](https://github.com/bruin-data/bruin) quality check scripts execute configured data quality validations. Failures stop the pipeline to avoid propagating poor-quality data.
4. Splink deduplicates the curated data, rewriting the clean dataset before upload to the Silver ADLS container.

### Gold: Serving Zone

1. DuckDB composes silver datasets into analytical models using SQL defined in the configuration.
2. Aggregated metrics are exported as Parquet to the Gold ADLS container for BI and downstream consumption.

## Getting Started

### Prerequisites

- Python 3.9+ (local development, packaging, and testing).
- Terraform >= 1.5.
- Azure CLI (logged in with an account that can create the resources below).
- Azure Functions Core Tools (optional but recommended for publishing and local testing).
- Access to Azure SQL and PostgreSQL sources, plus credentials that can be stored in Key Vault.

## Deployment Workflow

### 1. Configure

The configuration is split into two files:

- **`configs/initial.yml`**: Infrastructure settings (Key Vault, queue, sources, stage paths, monitoring). Copy and adjust:
  - Connection queries for Azure SQL and PostgreSQL sources.
  - Storage account names / container templates if you diverge from defaults.
  - Transformations path (default: `transformations`).
- **`configs/transformations.yml`**: [Bruin](https://github.com/bruin-data/bruin) transformation definitions for Silver and Gold stages. Lists which Python transformation scripts to run for each stage.

**Configuration Steps:**

1. Copy `configs/initial.yml` and `configs/transformations.yml` to your environment-specific configs.
2. Ensure the `key_vault.vault_url` points to the Vault provisioned by Terraform (or another of your choosing). Terraform seeds `queue-connection-string`, `application-insights-connection`, and `adls-storage-key`. Add remaining credentials (e.g. `azure-sql-password`, `postgres-password`) before execution.
3. Create [Bruin](https://github.com/bruin-data/bruin) transformation scripts in `transformations/` directory. Each script must define a `transform(con, inputs)` function that:
   - Receives a DuckDB connection (`con`) and input paths dictionary (`inputs`)
   - Returns a SQL query string (or pandas DataFrame) with the transformed data
   - Uses input aliases (e.g., `bronze_orders`, `orders_clean`) as DuckDB views
4. Create [Bruin](https://github.com/bruin-data/bruin) quality check scripts in `transformations/quality/` directory. Each script must define a `check(con, dataset_name)` function that:
   - Receives a DuckDB connection (`con`) with the dataset loaded as a view
   - Returns a tuple `(passed: bool, message: str)` indicating if checks passed
   - Performs data quality validations using SQL queries
5. Update `configs/transformations.yml` to list the [Bruin](https://github.com/bruin-data/bruin) transformations to run for each stage, along with quality check names and Splink settings for Silver transformations.
6. Any time you modify code inside `src/comboi`, rerun `python tools/embed_comboi.py` so that the vendored copy under `azure_functions/shared_packages/` stays in sync for Function deployments.
7. Optionally tailor `terraform/variables.tf` defaults (timer schedule, start stage, config path, region).
8. (Optional) Set up a local Python environment `pip install -e .` if you need to lint or unit-test custom logic.

### 2. Deploy with Terraform

```bash
cd terraform
terraform init
terraform apply -var="prefix=<yourprefix>" -var="environment=<env>"
```

The Terraform stack provisions:

- Resource group, Application Insights, Log Analytics workspace.
- Two storage accounts (one for Functions, one hierarchical namespace account for ADLS data + queue).
- Bronze/Silver/Gold Data Lake containers and the `comboi-tasks` Azure Storage Queue.
- Key Vault with secrets referencing the queue connection string and Application Insights connection string.
- Linux Consumption Function App, system-assigned managed identity, and required app settings.

After provisioning, publish the Azure Functions code (e.g. from the repo root):

```bash
python tools/embed_comboi.py  # copies src/comboi into azure_functions/shared_packages
cd azure_functions
# Ensure transformations directory is accessible (copy transformations/ directory or reference from parent)
func azure functionapp publish <function_app_name>
```

**Note**: The [Bruin](https://github.com/bruin-data/bruin) transformations directory (`transformations/`) must be accessible to the Azure Functions at runtime. Either:
- Copy the `transformations/` directory into `azure_functions/` before publishing, or
- Ensure the Function App can access the transformations via a mounted file share or by including it in the deployment package.

Terraform outputs the Function App name (`function_app_name`) and supporting resource identifiers to simplify this step. Redeployments only require re-running `terraform apply` and re-publishing if code changes.

### 3. Check the Scheduled Execution in Azure Monitor

- Verify the timer trigger (`driver` Function) runs according to `COMBOI_TIMER_SCHEDULE` by opening the Function App → Monitor blade.
- Inspect logs and traces via Application Insights (Log Analytics workspace). Useful Kusto queries:

```kusto
traces
| where cloud_RoleName == "<function_app_name>"
| where timestamp > ago(1h)
| order by timestamp desc
```

```kusto
requests
| where name contains "executor"
| summarize count() by bin(timestamp, 15m)
```

- Confirm queue-triggered executions complete for Bronze, Silver, and Gold stages (look for log statements `Stage <stage> completed`).
- Set alerts (optional) on failed runs or missing executions using Application Insights availability rules.

Once scheduled execution is healthy, downstream systems can consume Gold-layer outputs directly from ADLS.

## Operational Components

- **Driver** (`pipeline/driver.py`): Builds the stage task map, computes execution order, and supports both CLI and Function App invocations.
- **Queuer** (`pipeline/queue.py`): Wraps Azure Storage Queue operations and JSON payload handling.
- **Executor** (`pipeline/executor.py`): Runs stages sequentially with Rich progress feedback and streams status to the monitor.
- **Monitor** (`pipeline/monitoring.py`): Appends human-readable logs, persists metrics locally, and exports telemetry to Azure Monitor / Application Insights.
- **Azure Functions** (`azure_functions/driver`, `azure_functions/executor`): Timer-triggered scheduler enqueues medallion stages; queue-triggered executor runs each stage and chains the remainder.
- **Terraform** (`terraform/`): IaC definitions provisioning Function App, Key Vault, Storage accounts, queue, Application Insights, and supporting infrastructure.

## Extending the System

- **Add new sources**: Create additional connector classes and reference them in `configs/initial.yml`.
- **Add transformations**: Create new [Bruin](https://github.com/bruin-data/bruin) transformation scripts in `transformations/` (e.g., `transformations/my_transform.py` with a `transform(con, inputs)` function), then add them to `configs/transformations.yml`.
- **Add quality checks**: Create new [Bruin](https://github.com/bruin-data/bruin) quality check scripts in `transformations/quality/` (e.g., `transformations/quality/my_quality.py` with a `check(con, dataset_name)` function), then reference them in `configs/transformations.yml` under `quality_checks` for Silver transformations.
- **Add Splink deduplication**: Configure Splink settings in `configs/transformations.yml` for Silver transformations that need deduplication.
- **Integrate with other orchestration**: Reuse the Azure Functions entrypoints or invoke `Driver.run_stage()` programmatically.

## Troubleshooting

- **Missing DuckDB extensions**: Ensure DuckDB 0.10+ is installed. The pipeline auto-installs `odbc` and `postgres_scanner`, but network access may be required once per environment.
- **[Bruin](https://github.com/bruin-data/bruin) transformation errors**: Verify transformation scripts are in `transformations/` directory, check that each script defines a `transform(con, inputs)` function, and ensure transformation names in `configs/transformations.yml` match the Python file names (without `.py` extension).
- **[Bruin](https://github.com/bruin-data/bruin) quality check failures**: Verify quality check scripts are in `transformations/quality/` directory, check that each script defines a `check(con, dataset_name)` function that returns `(bool, str)`, and ensure quality check names in `configs/transformations.yml` match the Python file names (without `.py` extension). Review error messages in logs to identify which specific checks failed.
- **ADLS authentication issues**: Provide a valid credential in the configuration or export Azure identity context (e.g., `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`) before running the CLI.
- **Key Vault access issues**: Ensure the identity running `comboi` has `get` and `list` secret permissions on the referenced Key Vault and that the secret names in the configuration exist.
- **Azure Storage Queue problems**: Confirm the queue exists (it is created automatically if missing), the connection string is valid, and the identity has `send`, `receive`, and `delete` rights. Keep `host.json` queue batch size at one to prevent out-of-order execution.
- **Azure Functions binding errors**: Validate `COMBOI_QUEUE_NAME`, `COMBOI_QUEUE_CONNECTION`, and `COMBOI_TIMER_SCHEDULE` app settings and ensure they align with the YAML configuration.
- **Azure Monitor export issues**: Check that `AZURE_MONITOR_CONNECTION_STRING` or `monitoring.azure_connection_string` is present, the identity has `monitoring MetricsPublisher` permissions, and network rules allow ingestion.

## Roadmap Ideas

- Multicloud
- Industry Specific Building Blocks
- Streaming broker
- CDC
- Delta and Apache Iceberg Tables
---

Happy querying with DuckDB! Contributions and customizations are welcome.
Made in Berlin
