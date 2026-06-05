# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: fact_alertas_epidemiologicas | Lote: {lote_id}")
print("=" * 60)

df_enc,   version_enc   = leer_silver_cdf("HCE_ENCUENTROS")
df_sedes, version_sedes = leer_silver_cdf("RED_SEDES")

if df_enc is None and df_sedes is None:
    dbutils.notebook.exit("Sin cambios")

if df_enc   is None: df_enc   = leer_silver("HCE_ENCUENTROS")
if df_sedes is None: df_sedes = leer_silver("RED_SEDES")

df_sedes = df_sedes.select("id_sede","nom_ciudad","nom_pais")

# Enriquecer con ciudad
df = df_enc.join(df_sedes, on="id_sede", how="left")

# Agregar por semana, ciudad y CIE-10 (3 chars)
df_semanal = df \
    .withColumn("semana", F.weekofyear(F.col("fec_registro").cast("timestamp"))) \
    .withColumn("anio",   F.year(F.col("fec_registro").cast("timestamp"))) \
    .groupBy("anio","semana","nom_ciudad","diag_cie10_3char") \
    .agg(F.count("id_encuentro").alias("volumen_semana"))

# Calcular promedio móvil de las últimas 8 semanas por ciudad y CIE-10
window_8sem = Window \
    .partitionBy("nom_ciudad","diag_cie10_3char") \
    .orderBy("anio","semana") \
    .rowsBetween(-8, -1)

df_con_promedio = df_semanal \
    .withColumn("promedio_8_semanas",
                F.avg("volumen_semana").over(window_8sem)) \
    .withColumn("pct_desviacion",
                F.when(
                    F.col("promedio_8_semanas").isNotNull() &
                    (F.col("promedio_8_semanas") > 0),
                    F.round(
                        (F.col("volumen_semana") - F.col("promedio_8_semanas")) /
                        F.col("promedio_8_semanas") * 100, 2
                    )
                ).otherwise(F.lit(None).cast("double")))

# Filtrar solo alertas: desviación > 40% sobre el promedio
UMBRAL_BROTE = 40.0

df_alertas = df_con_promedio \
    .filter(
        F.col("pct_desviacion").isNotNull() &
        (F.col("pct_desviacion") > UMBRAL_BROTE)
    ) \
    .select(
        F.col("anio"),
        F.col("semana"),
        F.col("nom_ciudad").alias("ciudad"),
        F.col("diag_cie10_3char").alias("codigo_cie10"),
        F.col("volumen_semana").alias("volumen_actual"),
        F.round(F.col("promedio_8_semanas"), 2).alias("promedio_historico"),
        F.col("pct_desviacion").alias("pct_desviacion"),
        F.lit(datetime.now()).cast("timestamp").alias("fec_alerta"),
        F.lit("BROTE_EPIDEMIOLOGICO").alias("tipo_alerta"),
        F.lit(lote_id).alias("lote_id")
    )

n_alertas = df_alertas.count()
print(f"  🚨 Alertas epidemiológicas detectadas: {n_alertas:,}")

if n_alertas > 0:
    n = escribir_gold(
        df_alertas, "fact_alertas_epidemiologicas",
        pk_cols        = ["anio","semana","ciudad","codigo_cie10"],
        partition_cols = ["anio"],
        lote_id        = lote_id
    )
else:
    print("  ✅ Sin alertas en este período")
    n = 0

if version_enc is not None:
    update_version_cdf("HCE_ENCUENTROS", "silver", version_enc)
    print(f"  📌 CDF actualizado: silver/HCE_ENCUENTROS → v{version_enc}")
if version_sedes is not None:
    update_version_cdf("RED_SEDES", "silver", version_sedes)
    print(f"  📌 CDF actualizado: silver/RED_SEDES → v{version_sedes}")

duracion = (datetime.now() - inicio).seconds
log_gold("fact_alertas_epidemiologicas", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ fact_alertas_epidemiologicas completado | {n:,} alertas | {duracion}s")