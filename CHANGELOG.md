# CHANGELOG — HealthNet Data Pipeline

Todos los cambios significativos del proyecto están documentados en este archivo.
Formato basado en [Keep a Changelog](https://keepachangelog.com/es/1.0.0/).

---

## [2.0.0] — 2026-06-07 — alexandergenes

### Agregado
- Pipeline incremental completo y verificado con 2 ejecuciones consecutivas sin duplicados
- Tabla `cdf_versions` en `bronze/_control/` con MERGE idempotente — trackea versiones procesadas por capa
- `update_version_cdf` reescrito con MERGE (upsert) en lugar de UPDATE simple — resuelve el problema de versiones no registradas en primera ejecución
- Función `leer_bronze` en `silver_utils.py` usando `.table()` de Unity Catalog — resuelve permisos CDF con service principal de ADF
- Función `leer_silver_cdf` en `gold_utils.py` usando `.table()` de Unity Catalog
- Función `leer_gold_cdf` en `gold_utils.py` para lectura incremental desde Gold hacia KPIs
- Registro automático de tablas en Unity Catalog al momento de creación con permisos SELECT y MODIFY para service principal de ADF
- Log de ejecución Gold en `gold/_control/pipeline_log`
- Evidencias de ejecución en `docs/evidencias/`

### Modificado
- Dimensiones Gold (dim_pacientes, dim_medicos, dim_sedes) cambiadas a lectura completa `leer_silver()` — evita conflictos de CDF con tablas Silver compartidas
- `fact_alertas_epidemiologicas` cambiada a lectura completa — requiere todos los datos históricos para calcular promedio móvil de 8 semanas
- `gold_fact_alertas_epidemiologicas` escribe siempre aunque no haya alertas — garantiza que la tabla existe para KPIs
- `leer_bronze` en `silver_utils.py` usa `get_ultima_version_cdf(tabla, "silver")` en lugar de `"bronze"` — trackeo correcto por capa
- `leer_silver_cdf` en `gold_utils.py` usa `get_ultima_version_cdf(tabla, "gold")` en lugar de `"silver"`
- Inicialización de `cdf_versions` corregida a `capa="bronze"` en lugar de `"silver"`

### Corregido
- Error `StreamingQueryException` en facts Gold paralelas — pre-creación de tablas vacías con CDF y partición correcta
- Error `ProtocolChangedException` en ejecución paralela de Gold — tablas pre-creadas antes del pipeline
- Duplicados en dims Gold en segunda ejecución — corregido con lectura completa para dimensiones
- Registro huérfano `RED_SEDES/silver/-1` en `cdf_versions` — corregido en inicialización

### Limitaciones documentadas
- Facts con tablas Silver compartidas procesan completo en ejecución paralela
- Generador de datos sin offsets automáticos para segunda generación incremental
- Watermark truncado a segundos para compatibilidad con Azure SQL

---

## [1.5.0] — 2026-06-05 — alexandergenes

### Agregado
- CDF habilitado desde `v0` en todas las capas — evita error `CDF not recorded for version 0`
- Patrón de creación: tabla vacía con CDF habilitado → escritura de datos → CDF disponible desde v0
- `get_ultima_version_cdf` retorna `-1` cuando no existe registro — garantiza lectura desde primera versión

### Modificado
- `escribir_bronze`, `escribir_silver`, `escribir_gold` — crean tabla vacía con CDF antes de escribir datos
- `escribir_silver` y `escribir_gold` — eliminado `ALTER TABLE SET TBLPROPERTIES` redundante que generaba versión extra

### Corregido
- Error `CDF not recorded for version 0` — CDF se habilita en v0 antes de escribir datos
- Error `INSUFFICIENT_PERMISSIONS` con service principal ADF — uso de `.table()` en lugar de `.load(path)`
- Versiones CDF `2→2` sin cambios — eliminado ALTER TABLE redundante

---

## [1.4.0] — 2026-06-04 — alexandergenes

### Agregado
- Columna `fec_modificacion DATETIME DEFAULT GETDATE()` en las 7 tablas Azure SQL
- AutoLoader actualizado para agregar `fec_modificacion = current_timestamp()` al escribir en SQL
- Todos los notebooks Bronze actualizados a estrategia incremental — incluyendo RED_SEDES y MED_PLANTA
- `mergeSchema = true` en todas las capas — permite evolución de schema sin errores
- `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")` en Silver y Gold

### Modificado
- RED_SEDES y MED_PLANTA de `full_load` a `incremental`
- `watermark_col` de todas las tablas cambiado a `fec_modificacion`
- Watermark truncado a segundos + 1 segundo para evitar re-procesamiento

### Corregido
- Error `Conversion failed when converting date and/or time` — truncado de microsegundos en watermark
- Error `DOUBLE to Photon type long` — cast explícito de campos Decimal en AutoLoader
- Error `Column _rescued_data not found` — columna eliminada antes de escribir a SQL
- Error Primary Key duplicada en re-ejecuciones de AutoLoader

---

## [1.3.0] — 2026-06-03 — alexandergenes

### Agregado
- AutoLoader (`02_autoloader_landing_sql.py`) — reemplaza Event Trigger de ADF
- Soporte para múltiples archivos simultáneos con checkpoint exactamente-una-vez
- `alerta_anomalia_volumen.py` — detecta desviaciones > 30% con mínimo 3 ejecuciones históricas
- Pipeline `pl_autoloader` con trigger horario

### Modificado
- Event Trigger `tr_evento_landing` desactivado
- `pl_orquestador` incluye `alerta_anomalia_volumen` como último paso

### Corregido
- Falsos positivos en primera ejecución de alerta de volumen — validación de historial mínimo

---

## [1.2.0] — 2026-06-02 — alexandergenes

### Agregado
- Capa Gold completa: 3 dimensiones, 5 facts, 5 tablas KPIs
- `gold_utils.py` con funciones `escribir_gold`, `leer_silver`, `leer_silver_cdf`, `leer_gold_cdf`
- Linaje documentado: `grupo_edad`, `glosa_riesgo_epidemiologico`, `tasa_ocupacion_camas`
- Log de ejecución Gold
- MERGE idempotente en Gold

### Modificado
- `pl_gold_dims` y `pl_gold_facts` ejecutan en paralelo
- `pl_orquestador` con secuencia Bronze → Silver → Gold Dims → Gold Facts → Gold KPIs → Alerta

---

## [1.1.0] — 2026-06-01 — alexandergenes

### Agregado
- Capa Silver completa: 7 notebooks + `silver_utils.py`
- MERGE idempotente en Silver
- 5 pruebas de calidad automatizadas por tabla
- Enmascaramiento PII: `num_doc_hash` (SHA-256), `vr_facturado` y `vr_unitario` (NULL)
- Tabla de errores `silver/_control/errores_pipeline`
- Reporte de calidad `silver/_control/reporte_calidad`
- Validación de integridad referencial

---

## [1.0.0] — 2026-05-30 — alexandergenes

### Agregado
- Infraestructura Azure completa con Terraform
- Resource Group, Storage ADLS Gen2, Azure SQL, Key Vault, Log Analytics, Databricks Premium, ADF
- Backend remoto Terraform en ADLS Gen2
- Soporte multi-entorno: dev.tfvars y prod.tfvars
- Generación de datos sintéticos con seed=42, 7 tablas, distribuciones realistas
- 3 anomalías intencionales documentadas
- Múltiples formatos: CSV, Parquet, JSON
- Capa Bronze completa: 7 notebooks + bronze_utils.py
- Columnas de auditoría: `_ingesta_ts`, `_fuente`, `_lote_id`
- Particionamiento Bronze por `_anio/_mes/_dia`
- Unity Catalog con External Locations y Storage Credential
- Secret Scope referenciando Azure Key Vault
- Pipelines ADF: pl_bronze, pl_silver, pl_gold_dims, pl_gold_facts, pl_gold_kpis, pl_orquestador, pl_autoloader
- Trigger programado 02:00 AM UTC-5
- Alertas Azure Monitor: fallo (Sev 1) y éxito (Sev 4)
- Roles Azure AD: admin, data-engineer, analyst
- Catálogo de datos en docs/data_catalog.md
