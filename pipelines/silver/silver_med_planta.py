# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/03_silver/silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

def transformar_med_planta(df, tabla):
    """Transformaciones específicas de MED_PLANTA."""
    n_rechazados = 0

    # Calcular años de experiencia desde fec_ingreso
    df = df.withColumn(
        "anos_experiencia",
        F.round(
            F.datediff(F.current_date(), F.to_date(F.col("fec_ingreso"))) / 365.25,
            1
        )
    )

    # Castear anos_experiencia explícitamente a Double
    df = df.withColumn("anos_experiencia", F.col("anos_experiencia").cast("double"))

    # Estandarizar tip_contrato a catálogo controlado
    df = df.withColumn(
        "tip_contrato",
        F.when(F.col("tip_contrato") == "Planta",                F.lit("Planta"))
         .when(F.col("tip_contrato") == "Prestacion Servicios",   F.lit("Prestacion Servicios"))
         .when(F.col("tip_contrato") == "Honorarios",             F.lit("Honorarios"))
         .otherwise(F.lit("Otro"))
    )

    # Estandarizar jornada
    df = df.withColumn(
        "jornada",
        F.when(F.col("jornada") == "Completa",     F.lit("Completa"))
         .when(F.col("jornada") == "Medio Tiempo",  F.lit("Medio Tiempo"))
         .when(F.col("jornada") == "Turno",         F.lit("Turno"))
         .otherwise(F.lit("Otro"))
    )

    # Validar FK: id_sede debe existir en RED_SEDES Silver
    try:
        df_sedes = spark.read.format("delta") \
                        .load(f"{SILVER_BASE}/RED_SEDES") \
                        .select("id_sede")
        df, n_rechazados = validar_integridad_referencial(
            df, df_sedes, "id_sede", "id_sede", tabla, "RED_SEDES"
        )
    except Exception as e:
        print(f"  ⚠️  No se pudo validar FK id_sede: {str(e)[:80]}")

    return df, n_rechazados

# COMMAND ----------

REGLAS_CALIDAD = [
    {
        "nombre":    "med_id no nulo",
        "condicion": F.col("med_id").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "esp_principal no nulo",
        "condicion": F.col("esp_principal").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "anos_experiencia >= 0",
        "condicion": F.col("anos_experiencia") >= 0,
        "critica":   False
    },
    {
        "nombre":    "tip_contrato en catálogo",
        "condicion": F.col("tip_contrato").isin(
            ["Planta","Prestacion Servicios","Honorarios","Otro"]
        ),
        "critica": False
    },
    {
        "nombre":    "id_sede no nulo",
        "condicion": F.col("id_sede").isNotNull(),
        "critica":   True
    },
]

  


procesar_silver(
    tabla              = "MED_PLANTA",
    pk_cols            = ["med_id"],
    partition_col      = "fec_ingreso",
    estrategias_nulos  = {
        "esp_secundaria": ("imputar", "Sin Subespecialidad"),
        "jornada":        ("imputar", "No Informado"),
    },
    reglas_calidad      = REGLAS_CALIDAD,
    transformaciones_fn = transformar_med_planta
)