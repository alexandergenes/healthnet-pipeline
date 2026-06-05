# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/03_silver/silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

def transformar_age_citas(df, tabla):
    """Transformaciones específicas de AGE_CITAS."""
    n_rechazados = 0

    # Calcular tiempo de espera en minutos
    df = df.withColumn(
        "tiempo_espera_min",
        F.when(
            F.col("hra_llegada_paciente").isNotNull() &
            F.col("hra_inicio_atencion").isNotNull(),
            F.round(
                (F.unix_timestamp(F.col("hra_inicio_atencion").cast("timestamp")) -
                 F.unix_timestamp(F.col("hra_llegada_paciente").cast("timestamp"))) / 60,
                1
            )
        ).otherwise(F.lit(None).cast("double"))
    )

    # Registros con tiempo negativo → tabla de errores
    df_negativos = df.filter(
        F.col("tiempo_espera_min").isNotNull() &
        (F.col("tiempo_espera_min") < 0)
    )
    n_negativos = df_negativos.count()
    if n_negativos > 0:
        df_err = df_negativos \
            .withColumn("motivo_error", F.lit("TIEMPO_ESPERA_NEGATIVO")) \
            .withColumn("tabla_origen", F.lit(tabla)) \
            .withColumn("ts_error",     F.lit(datetime.now()).cast("timestamp"))
        registrar_error(df_err, tabla, "TIEMPO_NEGATIVO")
        # Marcar como nulo en lugar de excluir
        df = df.withColumn(
            "tiempo_espera_min",
            F.when(F.col("tiempo_espera_min") < 0, F.lit(None))
             .otherwise(F.col("tiempo_espera_min"))
        )
        n_rechazados += n_negativos
        print(f"  ⚠️  Tiempos negativos marcados como nulos: {n_negativos:,}")

    # Estandarizar estado_cita
    df = df.withColumn(
        "estado_cita",
        F.when(F.col("estado_cita") == "Atendida",   F.lit("Atendida"))
         .when(F.col("estado_cita") == "Cancelada",  F.lit("Cancelada"))
         .when(F.col("estado_cita") == "No Asistio", F.lit("No Asistio"))
         .when(F.col("estado_cita") == "Programada", F.lit("Programada"))
         .otherwise(F.lit("Otro"))
    )

    # Flag horario hábil (L-V 7:00-18:00)
    df = df.withColumn(
        "ind_horario_habil",
        F.when(
            (F.dayofweek(F.col("fec_cita_programada").cast("date")).between(2, 6)) &
            (F.hour(F.col("hra_cita_programada").cast("timestamp")).between(7, 17)),
            F.lit(1)
        ).otherwise(F.lit(0))
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
        "nombre":    "id_cita no nulo",
        "condicion": F.col("id_cita").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "pac_id no nulo",
        "condicion": F.col("pac_id").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "estado_cita en catálogo",
        "condicion": F.col("estado_cita").isin(
            ["Atendida","Cancelada","No Asistio","Programada","Otro"]
        ),
        "critica": False
    },
    {
        "nombre":    "fec_cita_programada no nula",
        "condicion": F.col("fec_cita_programada").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "tiempo_espera_min >= 0 o nulo",
        "condicion": F.col("tiempo_espera_min").isNull() |
                     (F.col("tiempo_espera_min") >= 0),
        "critica":   True
    },
]

procesar_silver(
    tabla              = "AGE_CITAS",
    pk_cols            = ["id_cita"],
    partition_col      = "fec_agendamiento",
    estrategias_nulos  = {
        "esp_solicitada":       ("imputar",   "No Informado"),
        "hra_llegada_paciente": ("indicador", None),
        "hra_inicio_atencion":  ("indicador", None),
    },
    reglas_calidad      = REGLAS_CALIDAD,
    transformaciones_fn = transformar_age_citas
)