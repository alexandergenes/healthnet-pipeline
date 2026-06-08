# HealthNet — Infraestructura como Código (Fase 2)

## Herramienta elegida: Terraform

**Justificación:** Terraform es agnóstico a la nube, tiene el ecosistema más maduro de providers para Azure, soporta múltiples entornos via workspaces y archivos de variables separados, y permite almacenar el estado de forma remota en ADLS Gen2.

## Recursos aprovisionados

| Recurso | Nombre (dev) | Propósito |
|---|---|---|
| Resource Group | `rg-healthnet-dev` | Contenedor de todos los recursos |
| Storage Account ADLS Gen2 | `dlshealthnetdev` | Lago de datos (landing/bronze/silver/gold) |
| Container landing | `landing` | Archivos crudos generados (CSV/Parquet/JSON) |
| Container bronze | `bronze` | Delta Lake — ingesta cruda desde SQL |
| Container silver | `silver` | Delta Lake — datos limpios |
| Container gold | `gold` | Delta Lake — modelo dimensional |
| Container tfstate | `tfstate` | Estado remoto de Terraform |
| Azure Key Vault | `kv-healthnet-dev` | Secretos y credenciales |
| Log Analytics Workspace | `law-healthnet-dev` | Logs y monitoreo |
| Action Group | `ag-healthnet-dev` | Alertas por email |
| Databricks Workspace | `dbw-healthnet-dev` | Motor de procesamiento PySpark |
| Azure Data Factory | `adf-healthnet-dev` | Orquestador de pipelines |

## Prerrequisitos

- Terraform >= 1.5.0
- Azure CLI >= 2.50
- Cuenta de Azure con suscripción activa
- Service Principal con rol Contributor

## Configuración inicial

### 1. Autenticación

```bash
az login
```

### 2. Variables de entorno del Service Principal

**Windows CMD:**
```cmd
set ARM_CLIENT_ID=<appId>
set ARM_CLIENT_SECRET=<password>
set ARM_SUBSCRIPTION_ID=<subscriptionId>
set ARM_TENANT_ID=<tenantId>
```

**Linux/Mac:**
```bash
export ARM_CLIENT_ID="<appId>"
export ARM_CLIENT_SECRET="<password>"
export ARM_SUBSCRIPTION_ID="<subscriptionId>"
export ARM_TENANT_ID="<tenantId>"
```

### 3. Archivo de credenciales

Copia `terraform.tfvars.example` a `terraform.tfvars` y completa:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edita `terraform.tfvars`:
```hcl
sql_admin_login    = "healthnet-admin"
sql_admin_password = "TU_PASSWORD_REAL"
```

⚠️ `terraform.tfvars` está en `.gitignore` — nunca se commitea.

## Despliegue

### Entorno DEV

```bash
cd infra/

# Inicializar Terraform
terraform init

# Planificar cambios
terraform plan -var-file="environments/dev/dev.tfvars"

# Aplicar
terraform apply -var-file="environments/dev/dev.tfvars"
```

### Entorno PROD

```bash
terraform plan -var-file="environments/prod/prod.tfvars"
terraform apply -var-file="environments/prod/prod.tfvars"
```

## Estado remoto

El estado de Terraform se almacena en:
```
dlshealthnetdev/tfstate/healthnet.terraform.tfstate
```

⚠️ El archivo `terraform.tfstate` **nunca** debe aparecer en el repositorio Git.

## Destruir infraestructura

```bash
terraform destroy -var-file="environments/dev/dev.tfvars"
```
