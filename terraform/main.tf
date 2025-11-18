locals {
  resource_group_name       = "${var.prefix}-rg-${var.environment}"
  function_storage_name     = lower(replace("${var.prefix}func${var.environment}", "/[^a-z0-9]/", ""))
  datalake_storage_name     = lower(replace("${var.prefix}lake${var.environment}", "/[^a-z0-9]/", ""))
  key_vault_name            = lower(replace("${var.prefix}-kv-${var.environment}", "/[^a-z0-9-]/", ""))
  app_insights_name         = "${var.prefix}-appi-${var.environment}"
  service_plan_name         = "${var.prefix}-plan-${var.environment}"
  function_app_name         = "${var.prefix}-func-${var.environment}"
  queue_name                = "comboi-tasks"
  bronze_container_name     = "bronze"
  silver_container_name     = "silver"
  gold_container_name       = "gold"
}

resource "azurerm_resource_group" "main" {
  name     = local.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "function" {
  name                     = local.function_storage_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_account" "datalake" {
  name                     = local.datalake_storage_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_kind             = "StorageV2"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_queue" "pipeline" {
  name                 = local.queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.prefix}-law-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

resource "azurerm_application_insights" "main" {
  name                = local.app_insights_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id        = azurerm_log_analytics_workspace.main.id
  application_type    = "web"
}

resource "azurerm_service_plan" "main" {
  name                = local.service_plan_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  os_type             = "Linux"
  sku_name            = "Y1"
}

resource "azurerm_linux_function_app" "main" {
  name                       = local.function_app_name
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  service_plan_id            = azurerm_service_plan.main.id
  storage_account_name       = azurerm_storage_account.function.name
  storage_account_access_key = azurerm_storage_account.function.primary_access_key
  functions_extension_version = var.function_runtime_version

  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }
    application_insights_key = azurerm_application_insights.main.instrumentation_key
    application_insights_connection_string = azurerm_application_insights.main.connection_string
  }

  app_settings = {
    FUNCTIONS_WORKER_RUNTIME          = "python"
    AzureWebJobsStorage               = azurerm_storage_account.function.primary_connection_string
    COMBOI_TIMER_SCHEDULE          = var.timer_schedule
    COMBOI_QUEUE_NAME              = local.queue_name
    COMBOI_QUEUE_CONNECTION        = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.queue_connection.value_uri})"
    COMBOI_CONFIG                  = var.config_path
    COMBOI_START_STAGE             = var.start_stage
    AZURE_MONITOR_CONNECTION_STRING   = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.appinsights_connection.value_uri})"
    DATA_LAKE_ACCOUNT_NAME            = azurerm_storage_account.datalake.name
    DATA_LAKE_URL                     = "https://${azurerm_storage_account.datalake.name}.dfs.core.windows.net"
  }
}

resource "azurerm_key_vault" "main" {
  name                        = local.key_vault_name
  location                    = azurerm_resource_group.main.location
  resource_group_name         = azurerm_resource_group.main.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  purge_protection_enabled    = false

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get",
      "Set",
      "List",
      "Delete",
      "Purge"
    ]
  }

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_linux_function_app.main.identity.principal_id

    secret_permissions = [
      "Get",
      "List"
    ]
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault_secret" "queue_connection" {
  name         = "queue-connection-string"
  value        = azurerm_storage_account.function.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "appinsights_connection" {
  name         = "application-insights-connection"
  value        = azurerm_application_insights.main.connection_string
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "adls_storage_key" {
  name         = "adls-storage-key"
  value        = azurerm_storage_account.datalake.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = local.bronze_container_name
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = local.silver_container_name
  storage_account_id = azurerm_storage_account.datalake.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = local.gold_container_name
  storage_account_id = azurerm_storage_account.datalake.id
}

