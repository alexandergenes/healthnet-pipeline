# ============================================================
# HealthNet — Terraform Main Configuration
# Fase 2: Infraestructura como Código
# Autor: Alexander Genes Manjarrez
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.50"
    }
  }

  # Backend remoto — estado en ADLS Gen2
  # El archivo terraform.tfstate NUNCA se commitea al repo
  backend "azurerm" {
    resource_group_name  = "rg-healthnet-dev"
    storage_account_name = "dlshealthnetdev"
    container_name       = "tfstate"
    key                  = "healthnet.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

provider "azuread" {}

# ─── Data sources ─────────────────────────────────────────────
data "azurerm_client_config" "current" {}

# ─── Resource Group ───────────────────────────────────────────
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location

  tags = local.common_tags
}

# ─── Storage Account (ADLS Gen2) ──────────────────────────────
resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Habilita ADLS Gen2

  tags = local.common_tags
}

# ─── Containers en ADLS Gen2 ──────────────────────────────────
resource "azurerm_storage_container" "landing" {
  name                  = "landing"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_name  = azurerm_storage_account.adls.name
  container_access_type = "private"
}

# ─── Azure Key Vault ──────────────────────────────────────────
resource "azurerm_key_vault" "kv" {
  name                = var.key_vault_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  purge_protection_enabled   = false
  soft_delete_retention_days = 7

  tags = local.common_tags
}

# Permisos Key Vault — Terraform SP
resource "azurerm_role_assignment" "kv_terraform_admin" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Permisos Key Vault — Databricks Resource Provider
resource "azurerm_role_assignment" "kv_databricks" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.databricks_resource_provider_object_id
}

# ─── Secrets en Key Vault ─────────────────────────────────────
resource "azurerm_key_vault_secret" "sql_server" {
  name         = "sql-server"
  value        = var.sql_server_fqdn
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_terraform_admin]
}

resource "azurerm_key_vault_secret" "sql_user" {
  name         = "sql-user"
  value        = var.sql_admin_login
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_terraform_admin]
}

resource "azurerm_key_vault_secret" "sql_password" {
  name         = "sql-password"
  value        = var.sql_admin_password
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_terraform_admin]
}

resource "azurerm_key_vault_secret" "storage_key" {
  name         = "storage-access-key"
  value        = azurerm_storage_account.adls.primary_access_key
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.kv_terraform_admin]
}

# ─── Log Analytics Workspace ──────────────────────────────────
resource "azurerm_log_analytics_workspace" "law" {
  name                = var.log_analytics_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "PerGB2018"
  retention_in_days   = 30

  tags = local.common_tags
}

# ─── Action Group para alertas ────────────────────────────────
resource "azurerm_monitor_action_group" "alerts" {
  name                = "ag-healthnet-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  short_name          = "hn-alerts"

  email_receiver {
    name                    = "pipeline-alerts"
    email_address           = var.alert_email
    use_common_alert_schema = true
  }

  tags = local.common_tags
}

# ─── Azure Databricks Workspace ───────────────────────────────
resource "azurerm_databricks_workspace" "dbw" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"

  tags = local.common_tags
}

# ─── Azure Data Factory ───────────────────────────────────────
resource "azurerm_data_factory" "adf" {
  name                = var.data_factory_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.common_tags
}

# Permisos ADF sobre Storage
resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# Permisos ADF sobre Key Vault
resource "azurerm_role_assignment" "adf_kv" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ─── Locals ───────────────────────────────────────────────────
locals {
  common_tags = {
    proyecto    = "healthnet-pipeline"
    environment = var.environment
    autor       = "Alexander Genes Manjarrez"
    fase        = "Fase2-IaC"
  }
}
