# CHANGELOG — HealthNet Data Pipeline

Todos los cambios significativos del proyecto están documentados en este archivo.
Formato basado en [Keep a Changelog](https://keepachangelog.com/es/1.0.0/).

---

## [1.5.0] — 2026-06-05 — alexandergenes

### Agregado
- Implementación de Change Data Feed (CDF) en todas las capas del pipeline (Bronze, Silver, Gold)
- CDF habilitado desde `v0` en la creación inicial de cada tabla Delta — evita el error `CDF not recorded for version 0`
- Tabla de control `cdf_versions` en `bronze/_control/` para trackear versiones procesadas por capa
- Función `leer_bronze()` en `silver_utils.py` usando `.table()` de Unity Catalog en lugar de `.load(path)` — resuelve permisos CDF con service principal de ADF
- Función `leer_silver_cdf()` en `gold_utils.py` usando `.table()` de Unity Catalog
- Función `leer_gold_cdf()` en `gold_utils.py` para lectura incremental desde Gold hacia KPIs
- Registro automático de tablas en Unity Catalog al momento de creación en `escribir_bronze()`, `escribir_silver()` y `escribir_gold()`
- Permisos SELECT y MODIFY otorgados automáticamente al service principal de ADF en cada tabla registrada

### Modificado
- `leer_bronze()` en `silver_utils.py`: usa `get_ultima_version_cdf(tabla, "silver")` en lugar de `"bronze"` — cada capa trackea cuánto ha procesado de la capa anterior
- `leer_silver_cdf()` en `gold_utils.py`: usa `get_ultima_version_cdf(tabla, "gold")` en lugar de `"silver"`
- `escribir_bronze()`: crea tabla vacía con CDF habilitado antes de escribir datos para garantizar CDF desde v0
- `escribir_silver()`: mismo patrón — tabla vacía con CDF antes de datos reales
- `escribir_gold()`: mismo patrón con soporte de partición correcta desde v0
- `get_ultima_version_cdf()`: retorna -1 cuando no existe registro (nunca procesado) en lugar de 0

---

## [1.4.0] — 2026-06-04 — alexandergenes

### Agregado
- Columna `fec_modificacion DATETIME DEFAULT GETDATE()` en las 7 tablas de Azure SQL — columna de control para ingesta incremental
- AutoLoader actualizado para agregar `fec_modificacion = current_timestamp()` al escribir en SQL
- Todos los notebooks Bronze actualizados a estrategia incremental usando `fec_modificacion` como `watermark_col` — incluye RED_SEDES y MED_PLANTA que antes eran full_load
- `mergeSchema = true` en `escribir_bronze()`, `escribir_silver()` y `escribir_gold()` — permite evolución de schema sin errores
- `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")` en Silver y Gold utils para MERGE con schema evolution

### Modificado
- RED_SEDES y MED_PLANTA cambiados de full_load a incremental — ahora todos los notebooks Bronze son incrementales
- `watermark_col` de todas las tablas cambiado a `fec_modificacion` — permite detectar tanto registros nuevos como modificados
- `partition_col` de RED_SEDES y MED_PLANTA actualizado a `fec_modificacion`

### Corregido
- Error `DOUBLE to Photon type long` en AutoLoader para tablas Parquet — cast explícito en `cargar_batch_a_sql()`
- Error `Column _rescued_data not found` — columna eliminada antes de escribir a SQL
- Error de Primary Key duplicada en re-ejecuciones de AutoLoader

---

## [1.3.0] — 2026-06-03 — alexandergenes

### Agregado
- AutoLoader (`02_autoloader_landing_sql.py`) — reemplaza el Event Trigger de ADF para ingesta desde landing/ hacia Azure SQL
- Soporte para múltiples archivos simultáneos con checkpoint en ADLS para garantizar exactamente-una-vez
- Notebook `alerta_anomalia_volumen.py` — detecta desviaciones mayor al 30% respecto al promedio histórico; requiere mínimo 3 ejecuciones para activarse
- Pipeline `pl_autoloader` en ADF con trigger horario `tr_autoloader_hourly`

### Modificado
- Event Trigger `tr_evento_landing` desactivado — reemplazado por AutoLoader
- `pl_orquestador` actualizado para incluir `alerta_anomalia_volumen` como último paso

### Corregido
- Error `ProtocolChangedException` en Gold — pre-creación de tablas vacías antes de ejecución paralela
- Validación de historial mínimo en alerta de volumen — evita falsos positivos en primera ejecución

---

## [1.2.0] — 2026-06-02 — alexandergenes

### Agregado
- Capa Gold completa: 3 dimensiones, 5 facts, 5 tablas KPIs ejecutivos
- `gold_utils.py` con funciones `escribir_gold()`, `leer_silver()`, `leer_silver_cdf()`, `leer_gold_cdf()`
- 13 notebooks Gold individuales
- Linaje de datos documentado para 3 campos calculados: `grupo_edad`, `glosa_riesgo_epidemiologico`, `tasa_ocupacion_camas`
- Particionamiento en Gold por dimensiones de análisis frecuentes
- MERGE idempotente en Gold

### Modificado
- Pipelines ADF `pl_gold_dims` y `pl_gold_facts` ejecutan notebooks en paralelo
- `pl_orquestador` con secuencia Bronze → Silver → Gold Dims → Gold Facts → Gold KPIs → Alerta Volumen

---

## [1.1.0] — 2026-06-01 — alexandergenes

### Agregado
- Capa Silver completa: 7 notebooks individuales + `silver_utils.py`
- MERGE idempotente en Silver — re-ejecuciones no generan duplicados
- 5 pruebas de calidad automatizadas por tabla
- Enmascaramiento PII: `num_doc_hash` (SHA-256), `vr_facturado` y `vr_unitario` (NULL en Silver)
- Tabla de errores `silver/_control/errores_pipeline`
- Tabla de reporte de calidad `silver/_control/reporte_calidad`
- Validación de integridad referencial con registro en tabla de errores
- Estrategia documentada de manejo de nulos por columna

---

## [1.0.0] — 2026-05-30 — alexandergenes

### Agregado
- Infraestructura Azure aprovisionada con Terraform
- Resource Group, Storage Account ADLS Gen2, Azure SQL Database, Key Vault, Log Analytics, Databricks Workspace Premium, Azure Data Factory
- Backend remoto Terraform en ADLS Gen2
- Soporte multi-entorno: dev.tfvars y prod.tfvars
- Generación de datos sintéticos con seed=42, 7 tablas, distribuciones realistas
- 3 anomalías intencionales: duplicados AGE_CITAS, fechas fuera de rango HCE, camas negativas GCM
- Múltiples formatos de salida: CSV, Parquet y JSON
- Script de carga a Azure SQL con manejo de FK
- Capa Bronze completa: 7 notebooks + bronze_utils.py
- Columnas de auditoría: _ingesta_ts, _fuente, _lote_id
- Particionamiento Bronze por _anio/_mes/_dia
- Unity Catalog con External Locations y Storage Credential
- Secret Scope referenciando Azure Key Vault
- Pipelines ADF: pl_bronze, pl_silver, pl_gold_dims, pl_gold_facts, pl_gold_kpis, pl_orquestador, pl_autoloader
- Trigger programado 02:00 AM UTC-5 Bogotá
- Alertas Azure Monitor: fallo (Sev 1) y éxito (Sev 4)
- Roles Azure AD: admin, data-engineer, analyst
- Catálogo de datos en /docs/data_catalog.md
