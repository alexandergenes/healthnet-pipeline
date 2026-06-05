# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/03_silver/silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

def transformar_red_sedes(df, tabla):
    """Transformaciones específicas de RED_SEDES."""
    n_rechazados = 0

    # Estandarizar tip_sede a catálogo controlado
    tipos_validos = [
        "Hospital Alta Complejidad",
        "Clinica Mediana Complejidad",
        "Centro Medico Ambulatorio",
        "Centro Diagnostico"
    ]
    df = df.withColumn(
        "tip_sede",
        F.when(F.col("tip_sede").isin(tipos_validos), F.col("tip_sede"))
         .otherwise(F.lit("Otro"))
    )

    # Calcular capacidad total de camas
    df = df.withColumn(
        "cap_camas_total",
        F.col("cap_camas_gen") +
        F.col("cap_camas_uci") +
        F.col("cap_camas_cirugia") +
        F.col("cap_camas_urg")
    )

    # Calcular porcentaje UCI sobre capacidad total
    df = df.withColumn(
        "pct_uci",
        F.when(F.col("cap_camas_total") > 0,
               F.round(F.col("cap_camas_uci") / F.col("cap_camas_total") * 100, 2))
         .otherwise(F.lit(0.0))
    )

    # Estandarizar nom_pais
    df = df.withColumn(
        "nom_pais",
        F.initcap(F.trim(F.col("nom_pais")))
    )

    return df, n_rechazados

# COMMAND ----------


REGLAS_CALIDAD = [
    {
        "nombre":    "id_sede no nulo",
        "condicion": F.col("id_sede").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "tip_sede en catálogo controlado",
        "condicion": F.col("tip_sede").isin([
            "Hospital Alta Complejidad","Clinica Mediana Complejidad",
            "Centro Medico Ambulatorio","Centro Diagnostico","Otro"
        ]),
        "critica": False
    },
    {
        "nombre":    "cap_camas_total >= 0",
        "condicion": F.col("cap_camas_total") >= 0,
        "critica":   True
    },
    {
        "nombre":    "nivel_complejidad entre 1 y 3",
        "condicion": F.col("nivel_complejidad").between(1, 3),
        "critica":   False
    },
    {
        "nombre":    "nom_pais no nulo",
        "condicion": F.col("nom_pais").isNotNull(),
        "critica":   True
    },
]

procesar_silver(
    tabla              = "RED_SEDES",
    pk_cols            = ["id_sede"],
    partition_col      = "fec_apertura",
    estrategias_nulos  = {
        "fec_apertura":   ("indicador", None),
        "nom_ciudad":     ("imputar",   "Sin Ciudad"),
    },
    reglas_calidad     = REGLAS_CALIDAD,
    transformaciones_fn = transformar_red_sedes
)

# COMMAND ----------

spark.read.format("delta").load(CDF_VERSIONS_PATH) \
     .filter(F.col("tabla") == "RED_SEDES") \
     .show(truncate=False)