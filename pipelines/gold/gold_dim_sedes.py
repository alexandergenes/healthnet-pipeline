# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: dim_sedes | Lote: {lote_id}")
print("=" * 60)

df, version_actual = leer_silver_cdf("RED_SEDES")

if df is None:
    dbutils.notebook.exit("Sin cambios")

df_dim = df.select(
    F.col("id_sede"),
    F.col("nom_sede"),
    F.col("tip_sede"),
    F.col("nivel_complejidad"),
    F.col("id_ciudad"),
    F.col("nom_ciudad"),
    F.col("id_pais"),
    F.col("nom_pais"),
    F.col("cap_camas_gen"),
    F.col("cap_camas_uci"),
    F.col("cap_camas_cirugia"),
    F.col("cap_camas_urg"),
    F.col("cap_camas_total"),
    F.col("pct_uci"),
    F.col("activa"),
    # Zona geográfica para análisis regional
    F.when(F.col("nom_pais") == "Colombia", F.lit("COL"))
     .when(F.col("nom_pais") == "Peru",     F.lit("PER"))
     .when(F.col("nom_pais") == "Ecuador",  F.lit("ECU"))
     .otherwise(F.lit("OTR")).alias("cod_pais"),
    # Clasificar capacidad
    F.when(F.col("cap_camas_total") >= 100, F.lit("Alta"))
     .when(F.col("cap_camas_total") >= 30,  F.lit("Media"))
     .otherwise(F.lit("Baja")).alias("categoria_capacidad")
)

n = escribir_gold(
    df_dim, "dim_sedes",
    pk_cols        = ["id_sede"],
    partition_cols = ["nom_pais"],
    lote_id        = lote_id
)

if version_actual is not None:
    update_version_cdf("RED_SEDES", "silver", version_actual)
    print(f"  📌 CDF actualizado: silver/RED_SEDES → v{version_actual}")

duracion = (datetime.now() - inicio).seconds
log_gold("dim_sedes", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ dim_sedes completado | {n:,} registros | {duracion}s")