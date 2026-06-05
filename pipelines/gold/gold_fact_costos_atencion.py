# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: fact_costos_atencion | Lote: {lote_id}")
print("=" * 60)

df_enc,  version_enc  = leer_silver_cdf("HCE_ENCUENTROS")
df_far,  version_far  = leer_silver_cdf("FAR_DISPENSACION")
df_pacs, version_pacs = leer_silver_cdf("PAC_REGISTRO")

if df_enc is None and df_far is None and df_pacs is None:
    dbutils.notebook.exit("Sin cambios")

if df_enc  is None: df_enc  = leer_silver("HCE_ENCUENTROS")
if df_far  is None: df_far  = leer_silver("FAR_DISPENSACION")
if df_pacs is None: df_pacs = leer_silver("PAC_REGISTRO")

df_pacs = df_pacs.select("pac_id","tip_aseguradora")

# Agregar costo de medicamentos por encuentro
df_far_agg = df_far \
    .filter(F.col("id_encuentro").isNotNull()) \
    .groupBy("id_encuentro") \
    .agg(
        F.sum(F.col("vr_unitario") * F.col("cantidad")).alias("vr_medicamentos"),
        F.count("id_dispensacion").alias("num_medicamentos")
    )

# Join encuentros con medicamentos
df_pac = leer_silver("PAC_REGISTRO").select("pac_id", "tip_aseguradora")

df_fact = df_enc \
    .join(df_far_agg, on="id_encuentro", how="left") \
    .join(df_pac,     on="pac_id",       how="left") \
    .select(
        F.col("id_encuentro"),
        F.col("pac_id"),
        F.col("id_sede"),
        F.col("fec_registro"),
        F.col("tip_consulta"),
        F.col("esp_atendida"),
        F.col("diag_cie10_3char"),
        F.col("tip_aseguradora"),
        F.col("estado_factura"),
        F.col("_nulo_vr_facturado"),
        F.coalesce(F.col("vr_medicamentos"), F.lit(0.0)).alias("vr_medicamentos"),
        F.coalesce(F.col("num_medicamentos"), F.lit(0)).alias("num_medicamentos"),
        F.coalesce(F.col("vr_medicamentos"), F.lit(0.0)).alias("costo_medicamentos"),
        F.year(F.col("fec_registro").cast("timestamp")).alias("anio"),
        F.month(F.col("fec_registro").cast("timestamp")).alias("mes"),
    )

n = escribir_gold(
    df_fact, "fact_costos_atencion",
    pk_cols        = ["id_encuentro"],
    partition_cols = ["anio","mes"],
    lote_id        = lote_id
)

if version_enc is not None:
    update_version_cdf("HCE_ENCUENTROS", "silver", version_enc)
    print(f"  📌 CDF actualizado: silver/HCE_ENCUENTROS → v{version_enc}")
if version_far is not None:
    update_version_cdf("FAR_DISPENSACION", "silver", version_far)
    print(f"  📌 CDF actualizado: silver/FAR_DISPENSACION → v{version_far}")
if version_pacs is not None:
    update_version_cdf("PAC_REGISTRO", "silver", version_pacs)
    print(f"  📌 CDF actualizado: silver/PAC_REGISTRO → v{version_pacs}")

duracion = (datetime.now() - inicio).seconds
log_gold("fact_costos_atencion", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ fact_costos_atencion completado | {n:,} registros | {duracion}s")