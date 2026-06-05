# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: kpis_ejecutivos | Lote: {lote_id}")
print("=" * 60)

# Leer todas las fuentes necesarias
df_consultas, version_consultas = leer_gold_cdf("fact_consultas")
df_camas,     version_camas     = leer_gold_cdf("fact_ocupacion_camas")
df_esperas,   version_esperas   = leer_gold_cdf("fact_tiempos_espera")
df_alertas,   version_alertas   = leer_gold_cdf("fact_alertas_epidemiologicas")

if all(df is None for df in [df_consultas, df_camas, df_esperas, df_alertas]):
    dbutils.notebook.exit("Sin cambios")

if df_consultas is None: df_consultas = spark.read.format("delta").load(f"{GOLD_BASE}/fact_consultas")
if df_camas     is None: df_camas     = spark.read.format("delta").load(f"{GOLD_BASE}/fact_ocupacion_camas")
if df_esperas   is None: df_esperas   = spark.read.format("delta").load(f"{GOLD_BASE}/fact_tiempos_espera")
if df_alertas   is None: df_alertas   = spark.read.format("delta").load(f"{GOLD_BASE}/fact_alertas_epidemiologicas")

print("  Fuentes Gold cargadas")

# COMMAND ----------

# ─── KPI 1: Ocupación de camas por sede y tipo de unidad ──────
kpi_ocupacion = df_camas \
    .groupBy("id_sede","nom_sede","nom_ciudad","nom_pais","tip_unidad","anio","mes") \
    .agg(
        F.round(F.avg("tasa_ocupacion"), 4).alias("tasa_ocupacion_promedio"),
        F.max("tasa_ocupacion").alias("tasa_ocupacion_maxima"),
        F.sum("ind_critico").alias("n_registros_criticos"),
        F.count("id_registro_cama").alias("n_snapshots")
    ) \
    .withColumn("kpi_tipo", F.lit("OCUPACION_CAMAS"))

print(f"  KPI 1 — Ocupación camas: {kpi_ocupacion.count():,} filas")

# COMMAND ----------

# ─── KPI 2: Tiempos de espera por sede y especialidad ─────────
kpi_esperas = df_esperas \
    .groupBy("id_sede","nom_ciudad","nom_pais","esp_solicitada","anio","mes") \
    .agg(
        F.round(F.avg("tiempo_espera_min"), 1).alias("tiempo_espera_promedio_min"),
        F.max("tiempo_espera_min").alias("tiempo_espera_maximo_min"),
        F.round(
            F.sum("ind_supera_umbral") / F.count("id_cita") * 100, 2
        ).alias("pct_supera_umbral_45min"),
        F.count("id_cita").alias("n_citas_atendidas")
    ) \
    .withColumn("kpi_tipo", F.lit("TIEMPOS_ESPERA"))

print(f"  KPI 2 — Tiempos espera: {kpi_esperas.count():,} filas")

# COMMAND ----------

# ─── KPI 3: Volumen consultas por ciudad, especialidad y mes ──
kpi_consultas = df_consultas \
    .groupBy("nom_ciudad","nom_pais","esp_atendida","tip_consulta","anio","mes") \
    .agg(
        F.count("id_encuentro").alias("n_consultas"),
        F.sum("ind_hospitalizacion").alias("n_hospitalizaciones"),
        F.sum("ind_dx_cronico").alias("n_dx_cronicos"),
        F.sum("ind_dx_oncologico").alias("n_dx_oncologicos"),
        F.sum("glosa_riesgo").alias("n_glosas_riesgo"),
        F.round(
            F.sum("glosa_riesgo") / F.count("id_encuentro") * 100, 2
        ).alias("pct_glosa_riesgo")
    ) \
    .withColumn("kpi_tipo", F.lit("VOLUMEN_CONSULTAS"))

print(f"  KPI 3 — Consultas: {kpi_consultas.count():,} filas")

# COMMAND ----------

# ─── KPI 4: Alertas epidemiológicas activas ───────────────────
kpi_alertas = df_alertas \
    .groupBy("ciudad","codigo_cie10","anio") \
    .agg(
        F.count("semana").alias("n_semanas_alerta"),
        F.max("pct_desviacion").alias("max_desviacion_pct"),
        F.max("volumen_actual").alias("volumen_maximo"),
        F.round(F.avg("promedio_historico"), 2).alias("promedio_historico")
    ) \
    .withColumn("kpi_tipo", F.lit("ALERTAS_EPIDEMIOLOGICAS"))

print(f"  KPI 4 — Alertas: {kpi_alertas.count():,} filas")

# COMMAND ----------

# ─── KPI 5: Resumen ejecutivo global por mes ──────────────────
kpi_ejecutivo = df_consultas \
    .groupBy("anio","mes") \
    .agg(
        F.count("id_encuentro").alias("total_consultas"),
        F.countDistinct("pac_id").alias("pacientes_unicos"),
        F.countDistinct("id_sede").alias("sedes_activas"),
        F.sum("ind_hospitalizacion").alias("total_hospitalizaciones"),
        F.round(
            F.sum("ind_hospitalizacion") / F.count("id_encuentro") * 100, 2
        ).alias("tasa_hospitalizacion_pct"),
        F.sum("glosa_riesgo").alias("total_glosas_riesgo"),
    ) \
    .join(
        df_esperas.groupBy("anio","mes").agg(
            F.round(F.avg("tiempo_espera_min"), 1).alias("tiempo_espera_promedio_min")
        ),
        on=["anio","mes"], how="left"
    ) \
    .join(
        df_camas.groupBy("anio","mes").agg(
            F.round(F.avg("tasa_ocupacion"), 4).alias("tasa_ocupacion_promedio_red")
        ),
        on=["anio","mes"], how="left"
    ) \
    .withColumn("kpi_tipo", F.lit("RESUMEN_EJECUTIVO"))

print(f"  KPI 5 — Ejecutivo: {kpi_ejecutivo.count():,} filas")

# COMMAND ----------

# ─── Escribir tabla KPIs ejecutivos ───────────────────────────
# Cada tipo de KPI se escribe en su propia tabla para optimizar consultas

tablas_kpi = {
    "kpis_ocupacion_camas":        (kpi_ocupacion, ["anio","mes","id_sede"],         ["anio","mes"]),
    "kpis_tiempos_espera":         (kpi_esperas,   ["anio","mes","id_sede"],         ["anio","mes"]),
    "kpis_volumen_consultas":      (kpi_consultas, ["anio","mes","nom_ciudad"],       ["anio","mes"]),
    "kpis_alertas_epidemiologicas":(kpi_alertas,   ["anio","ciudad","codigo_cie10"], ["anio"]),
    "kpis_resumen_ejecutivo":      (kpi_ejecutivo, ["anio","mes"],                   ["anio"]),
}

for nombre_tabla, (df_kpi, pks, partitions) in tablas_kpi.items():
    n = escribir_gold(df_kpi, nombre_tabla, pk_cols=pks,
                      partition_cols=partitions, lote_id=lote_id)
    log_gold(nombre_tabla, lote_id, n, "EXITOSO",
             (datetime.now() - inicio).seconds)
    
if version_consultas is not None:
    update_version_cdf("fact_consultas",         "gold", version_consultas)
if version_camas is not None:
    update_version_cdf("fact_ocupacion_camas",   "gold", version_camas)
if version_esperas is not None:
    update_version_cdf("fact_tiempos_espera",    "gold", version_esperas)
if version_alertas is not None:
    update_version_cdf("fact_alertas_epidemiologicas", "gold", version_alertas)

duracion = (datetime.now() - inicio).seconds
print(f"\n✅ kpis_ejecutivos completado | {duracion}s")
print("\n── KPIs disponibles para dashboard ──────────────────────")
print("  gold/kpis_ocupacion_camas        → Tasa ocupación por sede y unidad")
print("  gold/kpis_tiempos_espera         → Tiempos espera por especialidad")
print("  gold/kpis_volumen_consultas      → Consultas por ciudad y especialidad")
print("  gold/kpis_alertas_epidemiologicas→ Alertas CIE-10 por ciudad")
print("  gold/kpis_resumen_ejecutivo      → Dashboard ejecutivo mensual")