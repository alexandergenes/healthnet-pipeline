# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: dim_medicos | Lote: {lote_id}")
print("=" * 60)

df_med   = leer_silver("MED_PLANTA")
df_sedes = leer_silver("RED_SEDES")


df_sedes = df_sedes.select("id_sede","nom_sede","tip_sede","nom_ciudad","nom_pais")

# Join para enriquecer con datos de sede
df_dim = df_med.join(df_sedes, on="id_sede", how="left").select(
    F.col("med_id"),
    F.col("esp_principal"),
    F.col("esp_secundaria"),
    F.col("id_sede"),
    F.col("nom_sede"),
    F.col("tip_sede").alias("tip_sede_principal"),
    F.col("nom_ciudad").alias("ciudad_sede"),
    F.col("nom_pais").alias("pais_sede"),
    F.col("tip_contrato"),
    F.col("jornada"),
    F.col("estado_activo"),
    F.col("fec_ingreso"),
    F.col("anos_experiencia"),
    # Clasificar por nivel de experiencia
    F.when(F.col("anos_experiencia") < 2,  F.lit("Junior"))
     .when(F.col("anos_experiencia") < 5,  F.lit("Semior"))
     .when(F.col("anos_experiencia") < 10, F.lit("Senior"))
     .otherwise(F.lit("Experto")).alias("nivel_experiencia")
)

n = escribir_gold(
    df_dim, "dim_medicos",
    pk_cols        = ["med_id"],
    partition_cols = ["esp_principal"],
    lote_id        = lote_id
)


duracion = (datetime.now() - inicio).seconds
log_gold("dim_medicos", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ dim_medicos completado | {n:,} registros | {duracion}s")