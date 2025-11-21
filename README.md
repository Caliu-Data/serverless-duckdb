# Comboi - Serverless DuckDB ELT System in Azure

[![Repository](https://img.shields.io/badge/GitHub-Caliu--Data%2Fcomboi-blue)](https://github.com/Caliu-Data/comboi)

**Comboi** is a Python-based ELT (Extract, Load, Transform) system that implements a medallion architecture (Bronze → Silver → Gold) on top of DuckDB and Azure Data Lake Storage. It runs serverlessly using Azure Functions and is designed to be configuration-driven and easy to operate.

## 🚀 Key Features

- **Bronze Landing**: Incremental extracts from Azure SQL Database and PostgreSQL using DuckDB, persisted as Parquet in Azure Data Lake Storage (ADLS) with checkpointing to avoid full reloads
- **Silver Refinement**: Data cleansing using **Data Contracts** with [Bruin](https://github.com/bruin-data/bruin) quality checks and Splink-based deduplication, materialized back to ADLS
- **Gold Metrics**: Aggregations and business-ready metrics generated in DuckDB and delivered to ADLS
- **Data Contracts**: Declarative schema, quality rules, and SLA validation through YAML contracts
- **Serverless Execution**: Azure Functions with timer/queue triggers for automated pipeline orchestration
- **Secret Management**: Azure Key Vault integration for secure credential management
- **Structured Logging**: JSON logs using structlog for better observability
- **Infrastructure-as-Code**: Complete Azure infrastructure provisioned via Terraform

## 📁 Repository Structure

```
.
├── configs/                    # Pipeline configuration files
│   ├── initial.yml            # Infrastructure and source configuration
│   ├── transformations.yml     # Transformation and quality check definitions
│   └── default.yml            # Example configuration
├── contracts/                  # Data contract definitions (YAML)
│   ├── orders_clean.yml       # Example contract
│   └── customers_clean.yml     # Example contract
├── transformations/            # Bruin transformation scripts
│   ├── quality/               # Quality check scripts (legacy, use contracts instead)
│   └── *.py                   # Transformation scripts
├── src/comboi/                # Main application code
│   ├── connectors/            # Source connectors (Azure SQL, PostgreSQL)
│   ├── contracts/             # Data contract validation
│   ├── io/                    # ADLS client
│   ├── pipeline/              # Pipeline orchestration
│   ├── cli.py                 # CLI entry point
│   └── runner.py              # Driver factory
├── azure_functions/           # Azure Functions entrypoints
│   ├── driver/                # Timer-triggered scheduler
│   ├── executor/              # Queue-triggered executor
│   └── shared_packages/       # Vendored comboi package
├── terraform/                 # Infrastructure as Code
└── tools/                     # Utility scripts
    └── embed_comboi.py        # Copy comboi to Azure Functions
```

## 🏗️ Architecture

### Medallion Data Architecture

**Bronze (Landing Zone)**
- Extracts raw data from source systems (Azure SQL, PostgreSQL)
- Uses DuckDB extensions (`odbc`, `postgres_scanner`) for direct connection
- Incremental loads with checkpointing to avoid full reloads
- Persists as Parquet files in ADLS

**Silver (Curation Zone)**
- Reads Bronze Parquet files via DuckDB
- Applies [Bruin](https://github.com/bruin-data/bruin) transformations (Python functions)
- Validates data using **Data Contracts** (schema, quality rules, SLAs)
- Deduplicates using Splink
- Materializes cleaned data back to ADLS

**Gold (Serving Zone)**
- Composes Silver datasets into analytical models
- Generates business metrics and aggregations
- Exports as Parquet to ADLS for BI and downstream consumption

## 🚦 Quick Start

### Prerequisites

- **Python 3.9+** (for local development)
- **Terraform >= 1.5** (for infrastructure deployment)
- **Azure CLI** (logged in with appropriate permissions)
- **Azure Functions Core Tools** (optional, for local testing)
- Access to source databases and Azure resources

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd serverless-duckdb

# Install dependencies
pip install -e .
```

### Configuration

1. **Copy configuration templates:**
   ```bash
   cp configs/initial.yml configs/my-env.yml
   cp configs/transformations.yml configs/my-env-transformations.yml
   ```

2. **Configure sources** in `configs/my-env.yml`:
   - Update Key Vault URL
   - Configure Azure SQL and PostgreSQL connections
   - Set up source queries and incremental columns

3. **Configure transformations** in `configs/my-env-transformations.yml`:
   - Define Silver transformations
   - Reference data contracts using `contract:contract_name` in `quality_checks`
   - Configure Gold aggregations

4. **Create data contracts** in `contracts/`:
   - Define schema, quality rules, and SLAs
   - See `contracts/README.md` for contract format

5. **Create transformation scripts** in `transformations/`:
   - Each script must define a `transform(con, inputs)` function
   - Returns SQL query string or pandas DataFrame

### Local Testing

```bash
# Run the pipeline locally
comboi run all --config configs/my-env.yml

# Run a specific stage
comboi run silver --config configs/my-env.yml

# Plan execution without running
comboi plan --config configs/my-env.yml
```

## 🚀 Deployment

### 1. Deploy Infrastructure with Terraform

```bash
cd terraform
terraform init
terraform apply -var="prefix=myproject" -var="environment=prod"
```

This provisions:
- Resource group
- Storage accounts (Functions + ADLS)
- Bronze/Silver/Gold Data Lake containers
- Azure Storage Queue (`comboi-tasks`)
- Key Vault with queue connection string secret
- Linux Consumption Function App with managed identity

### 2. Configure Secrets in Key Vault

Add the following secrets to Key Vault:
- `queue-connection-string` (auto-created by Terraform)
- `adls-storage-key` (auto-created by Terraform)
- `azure-sql-password` (your database password)
- `postgres-password` (your database password)

### 3. Deploy Azure Functions

```bash
# Embed comboi package into Azure Functions
python tools/embed_comboi.py

# Copy transformations and contracts to Azure Functions
cp -r transformations azure_functions/
cp -r contracts azure_functions/
cp -r configs azure_functions/

# Deploy to Azure
cd azure_functions
func azure functionapp publish <function_app_name>
```

**Note**: The `transformations/` and `contracts/` directories must be accessible to Azure Functions at runtime. Include them in the deployment package or use a mounted file share.

### 4. Verify Execution

- Check Function App → Monitor for timer trigger execution
- Review logs (structured JSON via structlog)
- Verify queue-triggered executions complete for all stages
- Check log files at configured `log_path` for detailed execution logs

## 📝 Data Contracts

Data contracts provide declarative validation for your datasets. See `contracts/README.md` for complete documentation.

**Quick Example:**
```yaml
# contracts/orders_clean.yml
version: "1.0.0"
dataset: "orders_clean"
stage: "silver"

schema:
  columns:
    - name: order_id
      type: VARCHAR
      nullable: false
      constraints:
        - unique: true
        - not_null: true

quality_rules:
  - name: "no_duplicates"
    type: "uniqueness"
    column: "order_id"
    severity: "error"

sla:
  freshness:
    max_age_hours: 24
  completeness:
    min_row_count: 1
```

Reference contracts in `configs/transformations.yml`:
```yaml
silver:
  - name: orders_clean
    quality_checks:
      - contract:orders_clean  # References contracts/orders_clean.yml
```

## 🔧 Operational Components

- **Driver** (`pipeline/driver.py`): Orchestrates pipeline execution, builds task map, computes execution order
- **Executor** (`pipeline/executor.py`): Runs stages sequentially with progress tracking
- **Monitor** (`pipeline/monitoring.py`): Structured logging with structlog, metrics persistence
- **Queue** (`pipeline/queue.py`): Azure Storage Queue operations for stage chaining
- **Azure Functions**: Timer-triggered scheduler and queue-triggered executor

## 🛠️ Extending the System

### Add New Sources

1. Create a connector class in `src/comboi/connectors/`
2. Reference it in `configs/initial.yml` under `sources`

### Add Transformations

1. Create a Python script in `transformations/` with a `transform(con, inputs)` function
2. Add to `configs/transformations.yml` under the appropriate stage

### Add Data Contracts

1. Create a YAML file in `contracts/` defining schema, quality rules, and SLAs
2. Reference using `contract:contract_name` in `quality_checks` within `configs/transformations.yml`
3. See `contracts/README.md` for detailed contract format

### Add Quality Checks

**Recommended**: Use data contracts (see above)

**Legacy**: Create Python scripts in `transformations/quality/` with a `check(con, dataset_name)` function that returns `(bool, str)`

## 🐛 Troubleshooting

### Missing DuckDB Extensions
- Ensure DuckDB 0.10+ is installed
- Extensions (`odbc`, `postgres_scanner`) auto-install but require network access

### Transformation Errors
- Verify scripts are in `transformations/` directory
- Check that `transform(con, inputs)` function exists
- Ensure transformation names in config match Python file names (without `.py`)

### Data Contract Validation Failures
- Verify contract YAML files exist in `contracts/` directory
- Check contract name matches file name (without `.yml`)
- Ensure `contracts_path` is configured in `configs/initial.yml`
- Review validation error messages in logs

### Quality Check Failures
- For contracts: Check contract YAML syntax and validation rules
- For legacy checks: Verify scripts in `transformations/quality/` with `check(con, dataset_name)` function
- Review error messages in structured logs

### ADLS Authentication Issues
- Provide valid credential in configuration
- Or export Azure identity context: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`

### Key Vault Access Issues
- Ensure identity has `get` and `list` secret permissions
- Verify secret names exist in Key Vault
- Check Key Vault URL in configuration

### Azure Functions Issues
- Validate app settings: `COMBOI_QUEUE_NAME`, `COMBOI_QUEUE_CONNECTION`, `COMBOI_TIMER_SCHEDULE`
- Ensure `transformations/` and `contracts/` directories are included in deployment
- Check Function App logs for detailed error messages

### Logging Issues
- Verify `log_path` directory is writable
- Check that logs are being written (structured JSON format)
- Review Function App logs in Azure Portal

## 📚 Additional Resources

- **Data Contracts**: See `contracts/README.md` for contract documentation
- **Bruin**: [https://github.com/bruin-data/bruin](https://github.com/bruin-data/bruin)
- **DuckDB**: [https://duckdb.org/](https://duckdb.org/)
- **Splink**: [https://github.com/moj-analytical-services/splink](https://github.com/moj-analytical-services/splink)

## 🗺️ Roadmap

- Multicloud support
- Industry-specific building blocks
- Streaming broker integration
- Change Data Capture (CDC)
- Delta Lake and Apache Iceberg support

---

**Made in Berlin** | Contributions welcome!
