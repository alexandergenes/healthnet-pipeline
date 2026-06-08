# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: fact_consultas | Lote: {lote_id}")
print("=" * 60)

df_enc,   version_enc   = leer_silver_cdf("HCE_ENCUENTROS")
df_sedes, version_sedes = leer_silver_cdf("RED_SEDES")
df_pacs,  version_pacs  = leer_silver_cdf("PAC_REGISTRO")

if df_enc is None and df_sedes is None and df_pacs is None:
    dbutils.notebook.exit("Sin cambios")

if df_enc   is None: df_enc   = leer_silver("HCE_ENCUENTROS")
if df_sedes is None: df_sedes = leer_silver("RED_SEDES")
if df_pacs  is None: df_pacs  = leer_silver("PAC_REGISTRO")

df_sedes = df_sedes.select("id_sede","nom_ciudad","nom_pais","tip_sede")
df_pacs  = df_pacs.select("pac_id","grupo_edad","tip_aseguradora")

# Enriquecer con dimensiones
df_fact = df_enc \
    .join(df_sedes, on="id_sede", how="left") \
    .join(df_pacs,  on="pac_id",  how="left") \
    .select(
        F.col("id_encuentro"),
        F.col("pac_id"),
        F.col("med_id"),
        F.col("id_sede"),
        F.col("nom_ciudad"),
        F.col("nom_pais"),
        F.col("tip_sede"),
        F.col("fec_registro"),
        F.col("tip_consulta"),
        F.col("esp_atendida"),
        F.col("diag_principal_cie10"),
        F.col("diag_cie10_3char"),
        F.col("tiempo_estadia_horas"),
        F.col("glosa_riesgo"),
        F.col("estado_factura"),
        F.col("grupo_edad"),
        F.col("tip_aseguradora"),
        F.col("_nulo_vr_facturado"),
        # Año y mes para partición y análisis temporal
        F.year(F.col("fec_registro").cast("timestamp")).alias("anio"),
        F.month(F.col("fec_registro").cast("timestamp")).alias("mes"),
        # Flag hospitalización
        F.when(F.col("tip_consulta").isin(["Hospitalizacion","Cirugia"]),
               F.lit(1)).otherwise(F.lit(0)).alias("ind_hospitalizacion"),
        # Flag diagnóstico crónico
        F.when(F.col("diag_cie10_3char").isin(["I10","E11","J45","F32","I25","E14"]),
               F.lit(1)).otherwise(F.lit(0)).alias("ind_dx_cronico"),
        # Flag diagnóstico oncológico
        F.when(F.col("diag_cie10_3char").isin(["C50","C34","C18"]),
               F.lit(1)).otherwise(F.lit(0)).alias("ind_dx_oncologico")
    )

n = escribir_gold(
    df_fact, "fact_consultas",
    pk_cols        = ["id_encuentro"],
    partition_cols = ["anio","mes"],
    lote_id        = lote_id
)

if version_enc is not None:
    update_version_cdf("HCE_ENCUENTROS", "gold", version_enc)
    print(f"  📌 CDF actualizado: silver/HCE_ENCUENTROS → v{version_enc}")
if version_sedes is not None:
    update_version_cdf("RED_SEDES", "gold", version_sedes)
    print(f"  📌 CDF actualizado: silver/RED_SEDES → v{version_sedes}")
if version_pacs is not None:
    update_version_cdf("PAC_REGISTRO", "gold", version_pacs)
    print(f"  📌 CDF actualizado: silver/PAC_REGISTRO → v{version_pacs}")

duracion = (datetime.now() - inicio).seconds
log_gold("fact_consultas", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ fact_consultas completado | {n:,} registros | {duracion}s")