# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/03_silver/silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

# CIE-10 por especialidad — para flag glosa_riesgo
CIE10_POR_ESPECIALIDAD = {
    "Cardiologia":     ["I10","I25","I50","I21"],
    "Endocrinologia":  ["E11","E14","E10"],
    "Oncologia":       ["C50","C34","C18"],
    "Neurologia":      ["G40","G35","G43"],
    "Neumologia":      ["J45","J18","J44"],
    "Psiquiatria":     ["F32","F33","F20"],
}

def transformar_hce_encuentros(df, tabla):
    """Transformaciones específicas de HCE_ENCUENTROS."""
    n_rechazados = 0

    # ANOMALÍA 2: Fechas fuera de rango → tabla de errores
    df_fuera_rango = df.filter(
        F.col("fec_registro").cast("timestamp") < F.lit("2023-01-01").cast("timestamp")
    )
    n_fuera_rango = df_fuera_rango.count()
    if n_fuera_rango > 0:
        df_err = df_fuera_rango \
            .withColumn("motivo_error", F.lit("FECHA_FUERA_RANGO: fec_registro < 2023-01-01")) \
            .withColumn("tabla_origen", F.lit(tabla)) \
            .withColumn("ts_error",     F.lit(datetime.now()).cast("timestamp"))
        registrar_error(df_err, tabla, "FECHA_FUERA_RANGO")
        df = df.filter(
            F.col("fec_registro").cast("timestamp") >= F.lit("2023-01-01").cast("timestamp")
        )
        n_rechazados += n_fuera_rango
        print(f"  ⚠️  Fechas fuera de rango excluidas: {n_fuera_rango:,}")

    # Calcular tiempo de estadía en horas
    df = df.withColumn(
        "tiempo_estadia_horas",
        F.when(
            F.col("fec_egreso").isNotNull() & F.col("fec_registro").isNotNull(),
            F.round(
                (F.unix_timestamp(F.col("fec_egreso").cast("timestamp")) -
                 F.unix_timestamp(F.col("fec_registro").cast("timestamp"))) / 3600,
                2
            )
        ).otherwise(F.lit(None).cast("double"))
    )

    # Estandarizar CIE-10 a 3 caracteres para agrupación por capítulo
    df = df.withColumn(
        "diag_cie10_3char",
        F.substring(F.col("diag_principal_cie10"), 1, 3)
    )

    # Flag glosa_riesgo: CIE-10 no coincide con especialidad del médico
    # Construir mapa de especialidad → lista CIE-10 esperados
    condicion_glosa = F.lit(False)
    for especialidad, cie10_list in CIE10_POR_ESPECIALIDAD.items():
        condicion_glosa = condicion_glosa | (
            (F.col("esp_atendida") == especialidad) &
            (~F.col("diag_cie10_3char").isin(cie10_list))
        )

    df = df.withColumn(
        "glosa_riesgo",
        F.when(condicion_glosa, F.lit(1)).otherwise(F.lit(0))
    )

    # Estandarizar tip_consulta
    df = df.withColumn(
        "tip_consulta",
        F.when(F.col("tip_consulta") == "Primera Vez",      F.lit("Primera Vez"))
         .when(F.col("tip_consulta") == "Control",          F.lit("Control"))
         .when(F.col("tip_consulta") == "Urgencia",         F.lit("Urgencia"))
         .when(F.col("tip_consulta") == "Hospitalizacion",  F.lit("Hospitalizacion"))
         .when(F.col("tip_consulta") == "Cirugia",          F.lit("Cirugia"))
         .otherwise(F.lit("Otro"))
    )

    # Validar FKs
    try:
        df_pacs = spark.read.format("delta") \
                       .load(f"{SILVER_BASE}/PAC_REGISTRO").select("pac_id")
        df, n_fk = validar_integridad_referencial(
            df, df_pacs, "pac_id", "pac_id", tabla, "PAC_REGISTRO"
        )
        n_rechazados += n_fk
    except Exception as e:
        print(f"  ⚠️  FK pac_id no validada: {str(e)[:80]}")

    return df, n_rechazados

# COMMAND ----------


REGLAS_CALIDAD = [
    {
        "nombre":    "id_encuentro no nulo",
        "condicion": F.col("id_encuentro").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "diag_principal_cie10 no nulo",
        "condicion": F.col("diag_principal_cie10").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "fec_registro >= 2023-01-01",
        "condicion": F.col("fec_registro").cast("timestamp") >=
                     F.lit("2023-01-01").cast("timestamp"),
        "critica":   True
    },
    {
        "nombre":    "tiempo_estadia_horas >= 0 o nulo",
        "condicion": F.col("tiempo_estadia_horas").isNull() |
                     (F.col("tiempo_estadia_horas") >= 0),
        "critica":   False
    },
    {
        "nombre":    "tip_consulta en catálogo",
        "condicion": F.col("tip_consulta").isin([
            "Primera Vez","Control","Urgencia",
            "Hospitalizacion","Cirugia","Otro"
        ]),
        "critica": False
    },
]

procesar_silver(
    tabla              = "HCE_ENCUENTROS",
    pk_cols            = ["id_encuentro"],
    partition_col      = "fec_registro",
    estrategias_nulos  = {
        "vr_facturado":      ("indicador", None),
        "estado_factura":    ("imputar",   "Sin Estado"),
        "diag_sec1_cie10":   ("imputar",   "Z00"),
        "cod_procedimientos":("imputar",   "Sin Procedimiento"),
        "fec_egreso":        ("indicador", None),
    },
    reglas_calidad      = REGLAS_CALIDAD,
    transformaciones_fn = transformar_hce_encuentros
)