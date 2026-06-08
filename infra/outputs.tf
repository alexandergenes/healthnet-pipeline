# ============================================================
# HealthNet — Outputs de Terraform
# ============================================================

output "resource_group_name" {
  description = "Nombre del Resource Group"
  value       = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  description = "Nombre del Storage Account"
  value       = azurerm_storage_account.adls.name
}

output "storage_account_id" {
  description = "ID del Storage Account"
  value       = azurerm_storage_account.adls.id
}

output "adls_bronze_path" {
  description = "Path ABFSS del container bronze"
  value       = "abfss://bronze@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
}

output "adls_silver_path" {
  description = "Path ABFSS del container silver"
  value       = "abfss://silver@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
}

output "adls_gold_path" {
  description = "Path ABFSS del container gold"
  value       = "abfss://gold@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
}

output "adls_landing_path" {
  description = "Path ABFSS del container landing"
  value       = "abfss://landing@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
}

output "key_vault_name" {
  description = "Nombre del Key Vault"
  value       = azurerm_key_vault.kv.name
}

output "key_vault_uri" {
  description = "URI del Key Vault"
  value       = azurerm_key_vault.kv.vault_uri
}

output "key_vault_id" {
  description = "Resource ID del Key Vault"
  value       = azurerm_key_vault.kv.id
}

output "log_analytics_workspace_id" {
  description = "ID del Log Analytics Workspace"
  value       = azurerm_log_analytics_workspace.law.id
}

output "action_group_id" {
  description = "ID del Action Group para alertas"
  value       = azurerm_monitor_action_group.alerts.id
}

output "databricks_workspace_url" {
  description = "URL del workspace de Databricks"
  value       = azurerm_databricks_workspace.dbw.workspace_url
}

output "databricks_workspace_id" {
  description = "ID del workspace de Databricks"
  value       = azurerm_databricks_workspace.dbw.id
}

output "data_factory_name" {
  description = "Nombre del Azure Data Factory"
  value       = azurerm_data_factory.adf.name
}

output "data_factory_id" {
  description = "ID del Azure Data Factory"
  value       = azurerm_data_factory.adf.id
}
