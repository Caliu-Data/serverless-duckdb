output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "function_app_name" {
  value = azurerm_linux_function_app.main.name
}

output "storage_account_datalake" {
  value = azurerm_storage_account.datalake.name
}

output "queue_name" {
  value = azurerm_storage_queue.pipeline.name
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}


