## Terraform Stack

This module provisions the Azure footprint required for the `comboi` medallion ETL pipeline.

### Resources

- Resource group, Log Analytics workspace, Application Insights
- Storage account for Azure Functions (non-HNS)
- Storage account with hierarchical namespace for ADLS bronze/silver/gold layers
- Azure Storage Queue (`comboi-tasks`)
- Key Vault with secrets:
  - `queue-connection-string` (Function storage account connection string)
  - `application-insights-connection`
  - `adls-storage-key`
- Linux Consumption Function App with system-assigned managed identity
- Timer-trigger (driver) and queue-trigger (executor) app settings

### Usage

```bash
cd terraform
terraform init
terraform apply -var="prefix=<yourprefix>" -var="environment=<env>"
```

After deployment, publish the Azure Functions code from `azure_functions/` (e.g. `func azure functionapp publish <function_app_name>`). Add database credentials (`azure-sql-password`, `postgres-password`, etc.) to the created Key Vault before the first scheduled run.

### Variables

| Variable | Description | Default |
| --- | --- | --- |
| `prefix` | Short name prepended to all resources | (required) |
| `environment` | Environment suffix (dev, prod, etc.) | `dev` |
| `location` | Azure region | `eastus` |
| `function_runtime_version` | Azure Functions runtime version | `~4` |
| `timer_schedule` | CRON schedule for the timer trigger | `0 0 * * * *` |
| `start_stage` | Stage that the timer trigger starts from | `all` |
| `config_path` | Path to the YAML configuration in the Function App | `configs/default.yml` |

