# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: fact_ocupacion_camas | Lote: {lote_id}")
print("=" * 60)

df_camas, version_camas = leer_silver_cdf("GCM_CAMAS")
df_sedes, version_sedes = leer_silver_cdf("RED_SEDES")

if df_camas is None and df_sedes is None:
    dbutils.notebook.exit("Sin cambios")

if df_camas is None: df_camas = leer_silver("GCM_CAMAS")
if df_sedes is None: df_sedes = leer_silver("RED_SEDES")

df_sedes = df_sedes.select("id_sede","nom_sede","nom_ciudad","nom_pais",
                            "tip_sede","nivel_complejidad")

df_fact = df_camas \
    .join(df_sedes, on="id_sede", how="left") \
    .select(
        F.col("id_registro_cama"),
        F.col("id_sede"),
        F.col("nom_sede"),
        F.col("nom_ciudad"),
        F.col("nom_pais"),
        F.col("tip_sede"),
        F.col("nivel_complejidad"),
        F.col("tip_unidad"),
        F.col("fec_hora_registro"),
        F.col("num_camas_ocupadas"),
        F.col("num_camas_disp"),
        F.col("num_camas_mant"),
        F.col("tasa_ocupacion"),
        F.col("estado_ocupacion"),
        F.year(F.col("fec_hora_registro").cast("timestamp")).alias("anio"),
        F.month(F.col("fec_hora_registro").cast("timestamp")).alias("mes"),
        F.dayofmonth(F.col("fec_hora_registro").cast("timestamp")).alias("dia"),
        F.hour(F.col("fec_hora_registro").cast("timestamp")).alias("hora"),
        # Flag crítico por tipo de unidad con umbrales del config
        F.when(F.col("estado_ocupacion") == "Critico", F.lit(1))
         .otherwise(F.lit(0)).alias("ind_critico"),
        # Capacidad total disponible
        (F.col("num_camas_ocupadas") + F.col("num_camas_disp")).alias("cap_total_operativa")
    )

n = escribir_gold(
    df_fact, "fact_ocupacion_camas",
    pk_cols        = ["id_registro_cama"],
    partition_cols = ["anio","mes","tip_unidad"],
    lote_id        = lote_id
)

if version_camas is not None:
    update_version_cdf("GCM_CAMAS", "silver", version_camas)
    print(f"  📌 CDF actualizado: silver/GCM_CAMAS → v{version_camas}")
if version_sedes is not None:
    update_version_cdf("RED_SEDES", "silver", version_sedes)
    print(f"  📌 CDF actualizado: silver/RED_SEDES → v{version_sedes}")

duracion = (datetime.now() - inicio).seconds
log_gold("fact_ocupacion_camas", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ fact_ocupacion_camas completado | {n:,} registros | {duracion}s")