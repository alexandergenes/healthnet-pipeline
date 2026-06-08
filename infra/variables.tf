# ============================================================
# HealthNet — Variables de Terraform
# ============================================================

variable "environment" {
  description = "Entorno de despliegue"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "El entorno debe ser 'dev' o 'prod'."
  }
}

variable "location" {
  description = "Región de Azure para todos los recursos"
  type        = string
  default     = "eastus2"
}

variable "resource_group_name" {
  description = "Nombre del Resource Group"
  type        = string
}

variable "storage_account_name" {
  description = "Nombre del Storage Account (ADLS Gen2)"
  type        = string
}

variable "key_vault_name" {
  description = "Nombre del Azure Key Vault"
  type        = string
}

variable "log_analytics_name" {
  description = "Nombre del Log Analytics Workspace"
  type        = string
}

variable "databricks_workspace_name" {
  description = "Nombre del Azure Databricks Workspace"
  type        = string
}

variable "data_factory_name" {
  description = "Nombre del Azure Data Factory"
  type        = string
}

variable "alert_email" {
  description = "Email para recibir alertas del pipeline"
  type        = string
}

# ─── Azure SQL ────────────────────────────────────────────────
variable "sql_server_fqdn" {
  description = "FQDN del servidor Azure SQL (ej: sql-healthnet-dev.database.windows.net)"
  type        = string
}

variable "sql_admin_login" {
  description = "Usuario administrador de Azure SQL"
  type        = string
  sensitive   = true
}

variable "sql_admin_password" {
  description = "Contraseña administrador de Azure SQL"
  type        = string
  sensitive   = true
}
