# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/03_silver/silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

def transformar_pac_registro(df, tabla):
    """Transformaciones específicas de PAC_REGISTRO."""
    n_rechazados = 0

    # Calcular edad actual
    df = df.withColumn(
        "edad_actual",
        F.floor(
            F.datediff(F.current_date(), F.to_date(F.col("fec_nac"))) / 365.25
        )
    )

    # Calcular grupo de edad en rangos (especificación del documento)
    df = df.withColumn(
        "grupo_edad",
        F.when(F.col("edad_actual").between(0, 12),  F.lit("0-12"))
         .when(F.col("edad_actual").between(13, 17), F.lit("13-17"))
         .when(F.col("edad_actual").between(18, 40), F.lit("18-40"))
         .when(F.col("edad_actual").between(41, 65), F.lit("41-65"))
         .when(F.col("edad_actual") > 65,            F.lit("+65"))
         .otherwise(F.lit("Desconocido"))
    )

    # Estandarizar tip_aseguradora a catálogo controlado
    aseguradoras_validas = [
        "EPS Sanitas","EPS Sura","Nueva EPS","Compensar","Famisanar",
        "Salud Total","Coosalud","Medimas","Particular",
        "Rimac Seguros","Pacifico Seguros","La Positiva Seguros",
        "IESS Ecuador","Seguros Sucre"
    ]
    df = df.withColumn(
        "tip_aseguradora",
        F.when(F.col("tip_aseguradora").isin(aseguradoras_validas),
               F.col("tip_aseguradora"))
         .when(F.col("tip_aseguradora").isNull(), F.lit("No Informado"))
         .otherwise(F.lit("Otra Aseguradora"))
    )

    # Estandarizar genero
    df = df.withColumn(
        "genero",
        F.when(F.col("genero") == "M", F.lit("M"))
         .when(F.col("genero") == "F", F.lit("F"))
         .otherwise(F.lit("O"))
    )

    # Excluir registros sin pac_id (campo crítico)
    n_antes = df.count()
    df = df.filter(F.col("pac_id").isNotNull())
    n_rechazados += n_antes - df.count()

    return df, n_rechazados

# COMMAND ----------

REGLAS_CALIDAD = [
    {
        "nombre":    "pac_id no nulo",
        "condicion": F.col("pac_id").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "num_doc_hash longitud 64 (SHA-256)",
        "condicion": F.length(F.col("num_doc_hash")) == 64,
        "critica":   True
    },
    {
        "nombre":    "genero en M/F/O",
        "condicion": F.col("genero").isin(["M","F","O"]),
        "critica":   False
    },
    {
        "nombre":    "edad_actual entre 0 y 120",
        "condicion": F.col("edad_actual").between(0, 120),
        "critica":   False
    },
    {
        "nombre":    "grupo_edad asignado",
        "condicion": F.col("grupo_edad").isNotNull(),
        "critica":   False
    },
]

procesar_silver(
    tabla              = "PAC_REGISTRO",
    pk_cols            = ["pac_id"],
    partition_col      = "fec_primer_atencion",
    estrategias_nulos  = {
        "tip_aseguradora":    ("imputar",   "No Informado"),
        "estrato_socioec":    ("imputar",   0),
        "nom_ciudad_res":     ("imputar",   "No Informado"),
        "fec_primer_atencion":("indicador", None),
        "id_eps":             ("imputar",   "SIN_EPS"),
    },
    reglas_calidad      = REGLAS_CALIDAD,
    transformaciones_fn = transformar_pac_registro
)