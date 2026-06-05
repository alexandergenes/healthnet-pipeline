# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: dim_pacientes | Lote: {lote_id}")
print("=" * 60)

# Leer Silver
df, version_actual = leer_silver_cdf("PAC_REGISTRO")
if df is None:
    dbutils.notebook.exit("Sin cambios")

# Transformaciones Gold
df_dim = df.select(
    F.col("pac_id"),
    F.col("tip_doc"),
    F.col("num_doc_hash"),
    F.col("fec_nac"),
    F.col("genero"),
    F.col("grupo_edad"),
    F.col("edad_actual"),
    F.col("nom_ciudad_res"),
    F.col("tip_aseguradora"),
    F.col("id_eps"),
    F.col("estrato_socioec"),
    F.col("activo"),
    # Calcular antigüedad del paciente en la red
    F.when(
        F.col("fec_primer_atencion").isNotNull(),
        F.round(
            F.datediff(F.current_date(),
                       F.to_date(F.col("fec_primer_atencion"))) / 365.25, 1
        )
    ).otherwise(F.lit(0.0)).alias("anos_desde_primer_atencion"),
    # Perfil de complejidad inicial (se enriquece en dim_complejidad_paciente)
    F.when(F.col("estrato_socioec") <= 2, F.lit("Vulnerable"))
     .when(F.col("estrato_socioec") <= 4, F.lit("Medio"))
     .otherwise(F.lit("Alto")).alias("segmento_socioeconomico")
)

n = escribir_gold(
    df_dim, "dim_pacientes",
    pk_cols        = ["pac_id"],
    partition_cols = ["tip_aseguradora"],
    lote_id        = lote_id
)

if version_actual is not None:
    update_version_cdf("PAC_REGISTRO", "silver", version_actual)
    print(f"  📌 CDF actualizado: silver/PAC_REGISTRO → v{version_actual}")

duracion = (datetime.now() - inicio).seconds
log_gold("dim_pacientes", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ dim_pacientes completado | {n:,} registros | {duracion}s")