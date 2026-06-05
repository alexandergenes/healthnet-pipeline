# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/04_gold/gold_utils

# COMMAND ----------

from pyspark.sql import functions as F

lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
inicio  = datetime.now()

print("=" * 60)
print(f"  Gold: fact_tiempos_espera | Lote: {lote_id}")
print("=" * 60)

df_citas, version_citas = leer_silver_cdf("AGE_CITAS")
df_sedes, version_sedes = leer_silver_cdf("RED_SEDES")

if df_citas is None and df_sedes is None:
    dbutils.notebook.exit("Sin cambios")

if df_citas is None: df_citas = leer_silver("AGE_CITAS")
if df_sedes is None: df_sedes = leer_silver("RED_SEDES")

df_sedes = df_sedes.select("id_sede","nom_ciudad","nom_pais")

# Filtrar solo citas atendidas con tiempo de espera válido
# Registros con tiempo negativo ya fueron enviados a errores en Silver
df_atendidas = df_citas.filter(
    (F.col("estado_cita") == "Atendida") &
    F.col("tiempo_espera_min").isNotNull() &
    (F.col("tiempo_espera_min") >= 0)
)

n_excluidos = df_citas.count() - df_atendidas.count()
print(f"  Registros excluidos (no atendidos o sin tiempo): {n_excluidos:,}")

df_fact = df_atendidas \
    .join(df_sedes, on="id_sede", how="left") \
    .select(
        F.col("id_cita"),
        F.col("pac_id"),
        F.col("med_id"),
        F.col("id_sede"),
        F.col("nom_ciudad"),
        F.col("nom_pais"),
        F.col("esp_solicitada"),
        F.col("tip_cita"),
        F.col("fec_cita_programada"),
        F.col("tiempo_espera_min"),
        F.col("ind_horario_habil"),
        F.year(F.col("fec_cita_programada").cast("date")).alias("anio"),
        F.month(F.col("fec_cita_programada").cast("date")).alias("mes"),
        # Clasificar tiempo de espera
        F.when(F.col("tiempo_espera_min") <= 20,  F.lit("Optimo"))
         .when(F.col("tiempo_espera_min") <= 45,  F.lit("Aceptable"))
         .when(F.col("tiempo_espera_min") <= 120, F.lit("Alto"))
         .otherwise(F.lit("Critico")).alias("categoria_espera"),
        # Flag espera superior al umbral objetivo (45 min)
        F.when(F.col("tiempo_espera_min") > 45, F.lit(1))
         .otherwise(F.lit(0)).alias("ind_supera_umbral")
    )

n = escribir_gold(
    df_fact, "fact_tiempos_espera",
    pk_cols        = ["id_cita"],
    partition_cols = ["anio","mes"],
    lote_id        = lote_id
)

if version_citas is not None:
    update_version_cdf("AGE_CITAS", "silver", version_citas)
    print(f"  📌 CDF actualizado: silver/AGE_CITAS → v{version_citas}")
if version_sedes is not None:
    update_version_cdf("RED_SEDES", "silver", version_sedes)
    print(f"  📌 CDF actualizado: silver/RED_SEDES → v{version_sedes}")

duracion = (datetime.now() - inicio).seconds
log_gold("fact_tiempos_espera", lote_id, n, "EXITOSO", duracion)
print(f"\n✅ fact_tiempos_espera completado | {n:,} registros | {duracion}s")