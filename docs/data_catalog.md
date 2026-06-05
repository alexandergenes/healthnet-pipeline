# Catálogo de Datos — HealthNet Pipeline

**Proyecto:** HealthNet Data Pipeline  
**Plataforma:** Azure + Databricks  
**Última actualización:** 2026-06-05  
**Autor:** Alexander Genes Manjarrez

---

## Convenciones

| Indicador | Significado |
|---|---|
| 🔒 PII | Campo con Información de Identificación Personal |
| ⚠️ Nullable | Campo puede contener nulos (~5% en campos no críticos) |
| 🔑 PK | Clave primaria |
| 🔗 FK | Clave foránea |

---

## Capa Silver

### silver_RED_SEDES

**Descripción:** Catálogo de sedes de la red hospitalaria HealthNet. Tabla de dimensión maestra.  
**Origen:** `dbo.RED_SEDES` en Azure SQL  
**Watermark:** `fec_modificacion`  
**Partición:** `_anio / _mes / _dia` (por `fec_modificacion`)

| Campo | Tipo | PII | Nullable | Descripción |
|---|---|---|---|---|
| id_sede | Integer | No | No | 🔑 Identificador único de sede |
| nom_sede | String | No | No | Nombre oficial de la sede |
| tip_sede | String | No | No | Tipo: Hospital, Clínica, Centro Médico, IPS |
| id_ciudad | Integer | No | No | 🔗 ID de ciudad |
| nom_ciudad | String | No | No | Nombre de la ciudad (imputado: 'Sin Ciudad') |
| id_pais | Integer | No | No | 🔗 ID de país |
| nom_pais | String | No | No | Nombre del país |
| nivel_complejidad | Integer | No | No | Nivel 1, 2 o 3 |
| cap_camas_gen | Short | No | No | Capacidad camas general |
| cap_camas_uci | Short | No | No | Capacidad camas UCI |
| cap_camas_cirugia | Short | No | No | Capacidad camas cirugía |
| cap_camas_urg | Short | No | No | Capacidad camas urgencias |
| activa | Boolean | No | No | Estado activo de la sede |
| fec_apertura | Date | No | ⚠️ | Fecha de apertura de la sede |
| fec_modificacion | Timestamp | No | No | Timestamp de última modificación en SQL |
| _silver_ts | Timestamp | No | No | Timestamp de procesamiento Silver |
| _silver_lote | String | No | No | ID del lote de procesamiento Silver |

---

### silver_MED_PLANTA

**Descripción:** Médicos de planta por sede y especialidad.  
**Origen:** `dbo.MED_PLANTA`  
**Watermark:** `fec_modificacion`

| Campo | Tipo | PII | Nullable | Descripción |
|---|---|---|---|---|
| med_id | Integer | No | No | 🔑 Identificador único del médico |
| esp_principal | String | No | No | Especialidad principal |
| esp_secundaria | String | No | ⚠️ | Especialidad secundaria |
| id_sede | Integer | No | No | 🔗 ID de sede |
| fec_ingreso | Date | No | No | Fecha de ingreso a la red |
| tip_contrato | String | No | No | Tipo de contrato |
| jornada | String | No | No | Jornada: Completa, Parcial, Guardia |
| estado_activo | Boolean | No | No | Estado activo del médico |
| fec_modificacion | Timestamp | No | No | Timestamp de última modificación en SQL |

---

### silver_PAC_REGISTRO

**Descripción:** Registro maestro de pacientes de la red HealthNet.  
**Origen:** `dbo.PAC_REGISTRO`  
**Watermark:** `fec_modificacion`  
**Campos PII:** num_doc_hash

| Campo | Tipo | PII | Nullable | Descripción |
|---|---|---|---|---|
| pac_id | Integer | No | No | 🔑 Identificador único del paciente |
| tip_doc | String | No | No | Tipo de documento |
| num_doc_hash | String | 🔒 | No | Número de documento — SHA-256 aplicado en Silver |
| fec_nac | Date | No | ⚠️ | Fecha de nacimiento |
| genero | String | No | No | Género del paciente |
| id_ciudad_res | Integer | No | ⚠️ | ID ciudad de residencia |
| nom_ciudad_res | String | No | ⚠️ | Nombre ciudad de residencia |
| tip_aseguradora | String | No | ⚠️ | Tipo de aseguradora: EPS, Particular, ARL |
| id_eps | String | No | ⚠️ | ID de la EPS |
| estrato_socioec | Integer | No | ⚠️ | Estrato socioeconómico 1-6 |
| fec_primer_atencion | Date | No | ⚠️ | Fecha primera atención en la red |
| activo | Boolean | No | No | Estado activo del paciente |
| fec_modificacion | Timestamp | No | No | Timestamp de última modificación en SQL |

---

### silver_AGE_CITAS

**Descripción:** Agendamiento y estado de citas médicas.  
**Origen:** `dbo.AGE_CITAS`  
**Watermark:** `fec_modificacion`  
**Anomalías:** Duplicados intencionales (~0.5%) detectados y eliminados en Silver

| Campo | Tipo | PII | Nullable | Descripción |
|---|---|---|---|---|
| id_cita | Integer | No | No | 🔑 Identificador único de cita |
| pac_id | Integer | No | No | 🔗 ID paciente |
| med_id | Integer | No | No | 🔗 ID médico |
| id_sede | Integer | No | No | 🔗 ID sede |
| fec_agendamiento | Date | No | No | Fecha en que se agendó la cita |
| fec_cita_programada | Date | No | No | Fecha programada de la cita |
| hra_cita_programada | Timestamp | No | No | Hora programada |
| hra_llegada_paciente | Timestamp | No | ⚠️ | Hora de llegada del paciente |
| hra_inicio_atencion | Timestamp | No | ⚠️ | Hora de inicio de atención |
| esp_solicitada | String | No | No | Especialidad solicitada |
| tip_cita | String | No | No | Tipo: Primera Vez, Control, Urgencia |
| estado_cita | String | No | No | Estado: Atendida, Cancelada, No Asistió |
| fec_modificacion | Timestamp | No | No | Timestamp de última modificación en SQL |

---

### silver_HCE_ENCUENTROS

**Descripción:** Historia Clínica Electrónica — encuentros médicos.  
**Origen:** `dbo.HCE_ENCUENTROS`  
**Watermark:** `fec_modificacion`  
**Campos PII:** vr_facturado  
**Anomalías:** Fechas fuera de rango (~0.3%) excluidas en Silver

| Campo | Tipo | PII | Nullable | Descripción |
|---|---|---|---|---|
| id_encuentro | Integer | No | No | 🔑 Identificador único del encuentro |
| pac_id | Integer | No | No | 🔗 ID paciente |
| med_id | Integer | No | No | 🔗 ID médico |
| id_sede | Integer | No | No | 🔗 ID sede |
| fec_registro | Timestamp | No | No | Fecha y hora de registro |
| fec_inicio_atencion | Timestamp | No | ⚠️ | Fecha inicio de atención |
| fec_egreso | Timestamp | No | ⚠️ | Fecha de egreso |
| tip_consulta | String | No | No | Tipo de consulta |
| esp_atendida | String | No | No | Especialidad que atendió |
| diag_principal_cie10 | String | No | No | Diagnóstico principal CIE-10 |
| diag_sec1_cie10 | String | No | ⚠️ | Diagnóstico secundario CIE-10 |
| cod_procedimientos | String | No | ⚠️ | Códigos de procedimientos realizados |
| vr_facturado | Decimal(14,2) | 🔒 | ⚠️ | Valor facturado — NULL en Silver (PII financiero) |
| estado_factura | String | No | ⚠️ | Estado de facturación |
| fec_modificacion | Timestamp | No | No | Timestamp de última modificación en SQL |

---

### silver_GCM_CAMAS

**Descripción:** Gestión y ocupación de camas hospitalarias.  
**Origen:** `dbo.GCM_CAMAS`  
**Watermark:** `fec_modificacion`  
**Anomalías:** Camas negativas (~0.4%) excluidas en Silver

| Campo | Tipo | PII | Nullable | Descripción |
|---|---|---|---|---|
| id_registro_cama | Integer | No | No | 🔑 Identificador del registro |
| id_sede | Integer | No | No | 🔗 ID sede |
| tip_unidad | String | No | No | Tipo: General, UCI, Cirugía, Urgencias |
| fec_hora_registro | Timestamp | No | No | Fecha y hora del registro |
| num_camas_ocupadas | Short | No | No | Número de camas ocupadas |
| num_camas_disp | Short | No | No | Número de camas disponibles |
| num_camas_mant | Short | No | ⚠️ | Camas en mantenimiento |
| motivo_indisponibilidad | String | No | ⚠️ | Motivo de indisponibilidad |
| fec_modificacion | Timestamp | No | No | Timestamp de última modificación en SQL |

---

### silver_FAR_DISPENSACION

**Descripción:** Dispensación farmacéutica por encuentro médico.  
**Origen:** `dbo.FAR_DISPENSACION`  
**Watermark:** `fec_modificacion`  
**Campos PII:** vr_unitario

| Campo | Tipo | PII | Nullable | Descripción |
|---|---|---|---|---|
| id_dispensacion | Integer | No | No | 🔑 Identificador de dispensación |
| id_encuentro | Integer | No | ⚠️ | 🔗 ID encuentro HCE |
| pac_id | Integer | No | No | 🔗 ID paciente |
| id_sede | Integer | No | No | 🔗 ID sede |
| fec_dispensacion | Timestamp | No | No | Fecha y hora de dispensación |
| cod_medicamento | String | No | No | Código del medicamento |
| nom_medicamento | String | No | No | Nombre del medicamento |
| cantidad | Integer | No | No | Cantidad dispensada |
| vr_unitario | Decimal(10,2) | 🔒 | ⚠️ | Valor unitario — NULL en Silver (PII financiero) |
| tip_prescripcion | String | No | ⚠️ | Tipo de prescripción |
| fec_modificacion | Timestamp | No | No | Timestamp de última modificación en SQL |

---

## Capa Gold

### gold_dim_pacientes

**Descripción:** Dimensión de pacientes con atributos analíticos enriquecidos.  
**Origen:** silver_PAC_REGISTRO  
**Partición:** tip_aseguradora

| Campo | Tipo | Origen | Transformación | Propósito |
|---|---|---|---|---|
| pac_id | Integer | PAC_REGISTRO.pac_id | Directo | 🔑 Clave de dimensión |
| tip_doc | String | PAC_REGISTRO.tip_doc | Directo | Segmentación por tipo de documento |
| num_doc_hash | String | PAC_REGISTRO.num_doc_hash | SHA-256 aplicado en Silver | Identificación anonimizada |
| genero | String | PAC_REGISTRO.genero | Directo | Análisis demográfico |
| grupo_edad | String | PAC_REGISTRO.fec_nac | **Calculado:** edad = años desde fec_nac; grupo: 0-17/18-40/41-60/61+ | Segmentación etaria para análisis epidemiológico |
| tip_aseguradora | String | PAC_REGISTRO.tip_aseguradora | Directo | Análisis de cobertura |
| id_eps | String | PAC_REGISTRO.id_eps | Directo | Identificación de aseguradora |
| estrato_socioec | Integer | PAC_REGISTRO.estrato_socioec | Directo | Análisis socioeconómico |
| nom_ciudad_res | String | PAC_REGISTRO.nom_ciudad_res | Directo | Análisis geográfico |
| activo | Boolean | PAC_REGISTRO.activo | Directo | Filtro de pacientes activos |

**Linaje — grupo_edad:**  
Origen: `PAC_REGISTRO.fec_nac` → Silver: fecha validada → Gold: `edad = datediff(current_date, fec_nac) / 365`, categorización en rangos etarios estándar OPS → Propósito: segmentación demográfica para análisis epidemiológico y de utilización de servicios.

---

### gold_dim_medicos

**Descripción:** Dimensión de médicos con información de sede.  
**Origen:** silver_MED_PLANTA + silver_RED_SEDES  
**Partición:** esp_principal

| Campo | Tipo | Origen | Descripción |
|---|---|---|---|
| med_id | Integer | MED_PLANTA.med_id | 🔑 Clave de dimensión |
| esp_principal | String | MED_PLANTA.esp_principal | Especialidad principal |
| esp_secundaria | String | MED_PLANTA.esp_secundaria | Especialidad secundaria |
| tip_contrato | String | MED_PLANTA.tip_contrato | Tipo de contrato |
| jornada | String | MED_PLANTA.jornada | Jornada laboral |
| nom_sede | String | RED_SEDES.nom_sede | Nombre de sede de adscripción |
| tip_sede | String | RED_SEDES.tip_sede | Tipo de sede |
| nom_ciudad | String | RED_SEDES.nom_ciudad | Ciudad de la sede |
| nom_pais | String | RED_SEDES.nom_pais | País de la sede |
| estado_activo | Boolean | MED_PLANTA.estado_activo | Estado activo |

---

### gold_dim_sedes

**Descripción:** Dimensión completa de sedes con capacidades calculadas.  
**Origen:** silver_RED_SEDES  
**Partición:** nom_pais

| Campo | Tipo | Origen | Transformación | Propósito |
|---|---|---|---|---|
| id_sede | Integer | RED_SEDES.id_sede | Directo | 🔑 Clave de dimensión |
| nom_sede | String | RED_SEDES.nom_sede | Directo | Nombre de la sede |
| tip_sede | String | RED_SEDES.tip_sede | Directo | Clasificación de sede |
| nivel_complejidad | Integer | RED_SEDES.nivel_complejidad | Directo | Nivel 1, 2 o 3 |
| nom_ciudad | String | RED_SEDES.nom_ciudad | Directo | Ciudad |
| nom_pais | String | RED_SEDES.nom_pais | Directo | País |
| cap_camas_total | Integer | RED_SEDES.cap_* | **Calculado:** suma de cap_camas_gen + uci + cirugia + urg | Capacidad total para análisis de ocupación |
| pct_uci | Decimal | RED_SEDES.cap_* | **Calculado:** cap_camas_uci / cap_camas_total | Proporción UCI para clasificación de complejidad |
| activa | Boolean | RED_SEDES.activa | Directo | Filtro de sedes activas |

**Linaje — cap_camas_total:**  
Origen: `RED_SEDES.cap_camas_gen`, `cap_camas_uci`, `cap_camas_cirugia`, `cap_camas_urg` → Gold: suma de los 4 campos → Propósito: métrica base para cálculo de tasa de ocupación en fact_ocupacion_camas.

**Linaje — pct_uci:**  
Origen: `RED_SEDES.cap_camas_uci / cap_camas_total` → Gold: porcentaje redondeado a 2 decimales → Propósito: clasificación de sedes por intensidad de cuidados críticos.

---

### gold_fact_consultas

**Descripción:** Tabla de hechos de consultas médicas con tiempos y diagnósticos.  
**Origen:** silver_HCE_ENCUENTROS + silver_RED_SEDES + silver_PAC_REGISTRO  
**Partición:** anio, mes  
**Granularidad:** Un registro por encuentro médico

| Campo | Tipo | Origen | Descripción |
|---|---|---|---|
| id_encuentro | Integer | HCE.id_encuentro | 🔑 Clave del hecho |
| pac_id | Integer | HCE.pac_id | 🔗 Dimensión paciente |
| med_id | Integer | HCE.med_id | 🔗 Dimensión médico |
| id_sede | Integer | HCE.id_sede | 🔗 Dimensión sede |
| fec_registro | Timestamp | HCE.fec_registro | Fecha del encuentro |
| tip_consulta | String | HCE.tip_consulta | Tipo de consulta |
| esp_atendida | String | HCE.esp_atendida | Especialidad |
| diag_principal_cie10 | String | HCE.diag_principal_cie10 | Diagnóstico CIE-10 |
| duracion_min | Integer | HCE calculado | Minutos entre inicio y egreso |
| glosa_riesgo | String | HCE.diag_principal_cie10 | **Calculado:** clasificación de riesgo por código CIE-10 |
| nom_ciudad | String | RED_SEDES join | Ciudad de atención |
| grupo_edad | String | PAC_REGISTRO join | Grupo etario del paciente |
| anio | Integer | HCE.fec_registro | Año para partición |
| mes | Integer | HCE.fec_registro | Mes para partición |

**Linaje — glosa_riesgo:**  
Origen: `HCE_ENCUENTROS.diag_principal_cie10` → Gold: mapeo de prefijos CIE-10 a categorías de riesgo (A00-B99: Infeccioso, C00-D49: Neoplasias, E00-E90: Metabólico, etc.) → Propósito: segmentación de consultas por categoría de riesgo clínico para alertas epidemiológicas.

---

### gold_fact_ocupacion_camas

**Descripción:** Ocupación de camas hospitalarias por sede, unidad y período.  
**Origen:** silver_GCM_CAMAS + silver_RED_SEDES  
**Partición:** anio, mes, tip_unidad

| Campo | Tipo | Origen | Descripción |
|---|---|---|---|
| id_registro_cama | Integer | GCM.id_registro_cama | 🔑 Clave del hecho |
| id_sede | Integer | GCM.id_sede | 🔗 Dimensión sede |
| tip_unidad | String | GCM.tip_unidad | Tipo de unidad |
| fec_hora_registro | Timestamp | GCM.fec_hora_registro | Timestamp del registro |
| num_camas_ocupadas | Short | GCM.num_camas_ocupadas | Camas ocupadas |
| num_camas_disp | Short | GCM.num_camas_disp | Camas disponibles |
| tasa_ocupacion | Decimal | GCM calculado | **Calculado:** num_ocupadas / (ocupadas + disponibles) |
| nom_sede | String | RED_SEDES join | Nombre de la sede |
| nom_ciudad | String | RED_SEDES join | Ciudad |
| nivel_complejidad | Integer | RED_SEDES join | Nivel de complejidad |
| anio | Integer | GCM.fec_hora_registro | Año para partición |
| mes | Integer | GCM.fec_hora_registro | Mes para partición |

**Linaje — tasa_ocupacion:**  
Origen: `GCM_CAMAS.num_camas_ocupadas`, `num_camas_disp` → Gold: `tasa = num_ocupadas / (num_ocupadas + num_disponibles)`, redondeado a 4 decimales → Propósito: KPI principal de gestión hospitalaria para alertas de saturación.

---

### gold_fact_tiempos_espera

**Descripción:** Tiempos de espera en atención de citas médicas.  
**Origen:** silver_AGE_CITAS + silver_RED_SEDES  
**Partición:** anio, mes

| Campo | Tipo | Origen | Descripción |
|---|---|---|---|
| id_cita | Integer | AGE.id_cita | 🔑 Clave del hecho |
| pac_id | Integer | AGE.pac_id | 🔗 Dimensión paciente |
| med_id | Integer | AGE.med_id | 🔗 Dimensión médico |
| id_sede | Integer | AGE.id_sede | 🔗 Dimensión sede |
| fec_cita_programada | Date | AGE calculado | Fecha programada |
| esp_solicitada | String | AGE.esp_solicitada | Especialidad |
| tip_cita | String | AGE.tip_cita | Tipo de cita |
| estado_cita | String | AGE.estado_cita | Estado final |
| min_espera_llegada | Integer | AGE calculado | Minutos entre llegada e inicio atención |
| dias_espera_agendamiento | Integer | AGE calculado | Días entre agendamiento y cita |
| nom_ciudad | String | RED_SEDES join | Ciudad |
| anio | Integer | AGE.fec_cita_programada | Año para partición |
| mes | Integer | AGE.fec_cita_programada | Mes para partición |

---

### gold_kpis_resumen_ejecutivo

**Descripción:** KPIs ejecutivos consolidados de la red HealthNet.  
**Origen:** Todas las facts Gold  
**Granularidad:** Un registro por año-mes-país

| Campo | Descripción |
|---|---|
| anio | Año del período |
| mes | Mes del período |
| nom_pais | País |
| total_consultas | Total de encuentros médicos |
| consultas_urgencia | Consultas de tipo urgencia |
| pct_ocupacion_promedio | Tasa promedio de ocupación de camas |
| tiempo_espera_promedio_min | Tiempo promedio de espera en minutos |
| total_alertas_epidemiologicas | Número de alertas activas |
| total_dispensaciones | Total de dispensaciones farmacéuticas |

---

## Estrategia de manejo de nulos — Silver

| Tabla | Campo | Estrategia | Justificación |
|---|---|---|---|
| RED_SEDES | nom_ciudad | Imputar: 'Sin Ciudad' | Campo de referencia — valor desconocido aceptable |
| RED_SEDES | fec_apertura | Indicador binario _nulo_fec_apertura | Campo histórico — ausencia es informativa |
| PAC_REGISTRO | fec_nac | Indicador binario | Fecha de negocio relevante — ausencia es dato |
| PAC_REGISTRO | tip_aseguradora | Imputar: 'No Informado' | Campo de segmentación — necesario para Gold |
| AGE_CITAS | hra_llegada_paciente | Mantener null | Ausencia indica no presentación del paciente |
| HCE_ENCUENTROS | vr_facturado | NULL (PII) | Enmascaramiento de dato financiero sensible |
| GCM_CAMAS | num_camas_mant | Imputar: 0 | Sin registro de mantenimiento = 0 camas en mantenimiento |
| FAR_DISPENSACION | vr_unitario | NULL (PII) | Enmascaramiento de dato financiero sensible |
