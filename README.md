# HealthNet Data Pipeline — DataKnow Technical Assessment

**Candidato:** Alexander Genes Manjarrez  
**Sector:** C — Salud y Servicios Médicos  
**Plataforma:** Microsoft Azure + Databricks  
**Stack:** Databricks (DBR 13.3 LTS) + Delta Lake + Azure Data Factory + Terraform

---

## Sector y plataforma elegidos

**Sector — Salud y Servicios Médicos (HealthNet)**  
Se eligió el sector salud por la riqueza de los datos transaccionales: historias clínicas, agendamiento de citas, dispensación farmacéutica y gestión de camas generan múltiples patrones de ingesta heterogénea (CSV, Parquet, JSON) y reglas de negocio complejas con alto valor analítico. El escenario simula una red hospitalaria de 82 sedes en Colombia, Perú y Ecuador.

**Plataforma — Microsoft Azure + Databricks**  
Azure fue seleccionado por la integración nativa entre ADLS Gen2, Databricks y ADF, que permite implementar una arquitectura Medallion completa con Delta Lake, Unity Catalog y Change Data Feed sin fricciones. Databricks proporciona el motor de cómputo unificado para ingesta, transformación y analítica sobre el mismo formato Delta.

---

## Arquitectura

```
Azure SQL (fuente)
        ↓ AutoLoader (landing/ → SQL)
ADLS Gen2 landing/
        ↓ Bronze (watermark fec_modificacion)
ADLS Gen2 bronze/ — Delta Lake + CDF
        ↓ Silver (CDF incremental + MERGE)
ADLS Gen2 silver/ — Delta Lake + CDF
        ↓ Gold (CDF incremental + MERGE)
ADLS Gen2 gold/ — Delta Lake + CDF
        ↓
Power BI / Dashboards
```

**Orquestación:** Azure Data Factory — pl_orquestador (Schedule 02:00 AM UTC-5)  
**Gobierno:** Unity Catalog + Azure Key Vault + Azure AD Groups  
**Monitoreo:** Azure Monitor + Log Analytics

---

## Recursos Azure

| Recurso | Nombre | Región |
|---|---|---|
| Resource Group | rg-healthnet-dev | East US 2 |
| Storage Account ADLS Gen2 | dlshealthnetdev | East US 2 |
| Azure SQL Database | healthnet-source | Central US |
| Azure Key Vault | kv-healthnet-dev | East US 2 |
| Log Analytics Workspace | law-healthnet-dev | East US 2 |
| Databricks Workspace (Premium) | dbw-healthnet-dev | East US 2 |
| Azure Data Factory | adf-healthnet-dev | East US 2 |

**Containers ADLS:** landing, bronze, silver, gold, tfstate

---

## Tablas del modelo

| Tabla | Registros | Formato landing | Descripción |
|---|---|---|---|
| RED_SEDES | 82 | CSV | Catálogo de sedes de la red hospitalaria |
| MED_PLANTA | 2,000 | CSV | Médicos de planta por sede y especialidad |
| PAC_REGISTRO | 100,000+ | Parquet | Registro maestro de pacientes |
| AGE_CITAS | 150,000+ | Parquet | Agendamiento y atención de citas médicas |
| HCE_ENCUENTROS | 200,000+ | Parquet | Historia clínica electrónica — encuentros |
| GCM_CAMAS | 49,938+ | JSON | Gestión y ocupación de camas hospitalarias |
| FAR_DISPENSACION | 300,000+ | JSON | Dispensación farmacéutica |

---

## Decisiones técnicas clave

**AutoLoader en lugar de Event Trigger**  
El Event Trigger de ADF genera un pipeline run por archivo — inviable cuando múltiples archivos llegan simultáneamente desde la generación de datos. AutoLoader procesa lotes completos en un solo job, mantiene checkpoint en ADLS para garantizar exactamente-una-vez, y soporta schema evolution.

**fec_modificacion como watermark**  
Se agregó la columna `fec_modificacion DATETIME DEFAULT GETDATE()` a las 7 tablas de Azure SQL. El watermark de Bronze usa esta columna en lugar de fechas de negocio, lo que garantiza detectar tanto registros nuevos como modificados independientemente del rango temporal de los datos.

**Change Data Feed (CDF) en todas las capas**  
CDF se habilita desde `v0` de cada tabla Delta al momento de su creación. Esto permite que Silver lea únicamente los cambios de Bronze desde la última versión procesada, y Gold lea únicamente los cambios de Silver. La tabla `cdf_versions` en `bronze/_control/` trackea las versiones procesadas por capa.

**Unity Catalog con .table() en lugar de .load(path)**  
La lectura CDF via `readChangeFeed` requiere permisos de tabla registrada en Unity Catalog cuando se ejecuta desde el service principal de ADF. Todas las tablas Delta se registran automáticamente en `dbw_healthnet_dev.default` al momento de creación y se otorgan permisos SELECT y MODIFY al service principal de ADF.

**MERGE idempotente en Silver y Gold**  
Todas las escrituras en Silver y Gold usan operaciones MERGE (upsert) con la clave primaria de cada tabla. El pipeline puede ejecutarse múltiples veces sin generar duplicados.

**Schema evolution habilitada**  
`mergeSchema = true` y `spark.databricks.delta.schema.autoMerge.enabled = true` en todas las capas. Nuevas columnas en la fuente SQL se propagan automáticamente sin intervención manual.

**Key Vault para todos los secretos**  
Ninguna credencial, token o contraseña aparece en el código. Todos los secretos se referencian desde el Secret Scope `healthnet-kv-scope` que apunta a `kv-healthnet-dev`.

---

## Estructura del repositorio

```
healthnet-pipeline/
├── README.md                          # Este archivo
├── CHANGELOG.md                       # Historial de cambios
├── .gitignore                         # Excluye secretos y estado Terraform
├── data-generation/
│   ├── 00_subir_config_yaml.py        # Sube generation_config.yaml a ADLS
│   ├── 01_fase_generacion_datos.py    # Genera datos sintéticos → landing/
│   ├── 02_autoloader_landing_sql.py   # AutoLoader landing/ → Azure SQL
│   └── generation_config.yaml        # Parámetros: volumen, fechas, seed
├── infra/
│   ├── main.tf                        # Recursos Azure
│   ├── variables.tf                   # Variables parametrizadas
│   ├── outputs.tf                     # Outputs exportados
│   ├── terraform.tfvars.example       # Plantilla sin credenciales
│   ├── README.md                      # Instrucciones de despliegue
│   └── environments/
│       ├── dev/dev.tfvars             # Variables entorno dev
│       └── prod/prod.tfvars           # Variables entorno prod
├── pipelines/
│   ├── bronze/
│   │   ├── bronze_utils.py            # Funciones: ingestar_bronze, escribir_bronze, CDF
│   │   ├── bronze_red_sedes.py
│   │   ├── bronze_med_planta.py
│   │   ├── bronze_pac_registro.py
│   │   ├── bronze_age_citas.py
│   │   ├── bronze_hce_encuentros.py
│   │   ├── bronze_gcm_camas.py
│   │   └── bronze_far_dispensacion.py
│   ├── silver/
│   │   ├── silver_utils.py            # Funciones: leer_bronze CDF, escribir_silver, calidad
│   │   ├── silver_red_sedes.py
│   │   ├── silver_med_planta.py
│   │   ├── silver_pac_registro.py
│   │   ├── silver_age_citas.py
│   │   ├── silver_hce_encuentros.py
│   │   ├── silver_gcm_camas.py
│   │   └── silver_far_dispensacion.py
│   └── gold/
│       ├── gold_utils.py              # Funciones: leer_silver_cdf, leer_gold_cdf, escribir_gold
│       ├── gold_dim_pacientes.py
│       ├── gold_dim_medicos.py
│       ├── gold_dim_sedes.py
│       ├── gold_fact_consultas.py
│       ├── gold_fact_ocupacion_camas.py
│       ├── gold_fact_tiempos_espera.py
│       ├── gold_fact_costos_atencion.py
│       ├── gold_fact_alertas_epidemiologicas.py
│       ├── gold_kpis_ejecutivos.py
│       └── alerta_anomalia_volumen.py
├── orchestration/
│   └── adf_pipelines/                 # JSONs exportados de ADF
│       ├── pl_bronze.json
│       ├── pl_silver.json
│       ├── pl_gold_dims.json
│       ├── pl_gold_facts.json
│       ├── pl_gold_kpis.json
│       ├── pl_orquestador.json
│       └── pl_autoloader.json
└── docs/
    ├── data_catalog.md                # Catálogo: Silver + Gold con tipos, PII, linaje
    └── er_diagram                     # Diagrama ER del modelo relacional
```

---

## Guía de despliegue

### Prerequisitos

- Azure CLI instalado y autenticado (`az login`)
- Terraform >= 1.5.0
- Python >= 3.9
- Databricks CLI configurado

### 1. Infraestructura

```bash
cd infra/
cp terraform.tfvars.example terraform.tfvars
# Editar terraform.tfvars con tus valores (no commitear)

terraform init
terraform plan -var-file="environments/dev/dev.tfvars"
terraform apply -var-file="environments/dev/dev.tfvars"
```

### 2. Configurar secretos en Key Vault

```bash
az keyvault secret set --vault-name kv-healthnet-dev --name sql-server --value "<server>"
az keyvault secret set --vault-name kv-healthnet-dev --name sql-user --value "healthnet-admin"
az keyvault secret set --vault-name kv-healthnet-dev --name sql-password --value "<password>"
az keyvault secret set --vault-name kv-healthnet-dev --name storage-access-key --value "<key>"
az keyvault secret set --vault-name kv-healthnet-dev --name databricks-token --value "<token>"
```

### 3. Generación de datos

En Databricks, ejecutar en orden:
```
1. data-generation/00_subir_config_yaml        → Sube generation_config.yaml a ADLS
2. data-generation/01_fase_generacion_datos     → Genera archivos en landing/
3. data-generation/02_autoloader_landing_sql    → Carga datos a Azure SQL
```

### 4. Ejecutar pipeline

Desde ADF → **pl_orquestador** → **Trigger now**

O esperar el trigger automático diario a las 02:00 AM UTC-5.

---

## Pipelines ADF

| Pipeline | Descripción |
|---|---|
| `pl_bronze` | 7 notebooks Bronze en paralelo |
| `pl_silver` | 7 notebooks Silver con dependencias FK |
| `pl_gold_dims` | 3 dimensiones en paralelo |
| `pl_gold_facts` | 5 facts en paralelo |
| `pl_gold_kpis` | KPIs ejecutivos |
| `pl_orquestador` | Orquesta Bronze → Silver → Gold → Alerta Volumen |
| `pl_autoloader` | AutoLoader landing → SQL (trigger horario) |

---

## Roles y accesos

| Rol | Grupo Azure AD | Permisos |
|---|---|---|
| Administrador | grp-healthnet-admin | Owner en Resource Group |
| Ingeniero de Datos | grp-healthnet-data-engineer | Contributor + Storage Blob Data Contributor |
| Analista | grp-healthnet-analyst | Reader + Storage Blob Data Reader solo en gold/ |

---

## Monitoreo y alertas

| Alerta | Trigger | Severidad |
|---|---|---|
| Fallo del pipeline | Cualquier actividad ADF falla | Sev 1 — inmediata |
| Reporte diario de éxito | Pipeline completa exitosamente | Sev 4 — informativa |
| Anomalía de volumen | Desviación > 30% vs promedio histórico | Sev 2 — en pipeline |

Monitoreo en: **ADF → Monitor → Pipeline runs**  
Logs en: **Log Analytics Workspace — law-healthnet-dev**

---

## Tablas de control Delta

| Path | Descripción |
|---|---|
| `bronze/_control/pipeline_watermark` | Último timestamp procesado por tabla |
| `bronze/_control/pipeline_log` | Log de cada ejecución Bronze |
| `bronze/_control/cdf_versions` | Versiones CDF procesadas por capa |
| `silver/_control/errores_pipeline` | Registros rechazados con motivo |
| `silver/_control/reporte_calidad` | Métricas de calidad por ejecución |
| `silver/_control/pruebas_calidad` | Resultados de las 5 pruebas automatizadas |
