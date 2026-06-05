# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/03_silver/silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

def transformar_gcm_camas(df, tabla):
    """Transformaciones específicas de GCM_CAMAS."""
    n_rechazados = 0

    # ANOMALÍA 3: num_camas_disp negativo → tabla de errores
    df_inconsistentes = df.filter(F.col("num_camas_disp") < 0)
    n_inconsistentes  = df_inconsistentes.count()
    if n_inconsistentes > 0:
        df_err = df_inconsistentes \
            .withColumn("motivo_error",
                        F.lit("CAMAS_DISP_NEGATIVA: num_camas_disp < 0")) \
            .withColumn("tabla_origen", F.lit(tabla)) \
            .withColumn("ts_error",     F.lit(datetime.now()).cast("timestamp"))
        registrar_error(df_err, tabla, "CAMAS_NEGATIVAS")
        df = df.filter(F.col("num_camas_disp") >= 0)
        n_rechazados += n_inconsistentes
        print(f"  ⚠️  Registros con camas negativas excluidos: {n_inconsistentes:,}")

    # Calcular tasa de ocupación
    df = df.withColumn(
        "tasa_ocupacion",
        F.when(
            (F.col("num_camas_ocupadas") + F.col("num_camas_disp")) > 0,
            F.round(
                F.col("num_camas_ocupadas") /
                (F.col("num_camas_ocupadas") + F.col("num_camas_disp")),
                4
            )
        ).otherwise(F.lit(0.0))
    )

    # Clasificar estado de ocupación según umbrales del config
    # UCI: crítico > 85%, Urgencias: > 90%, General/Cirugia: > 88%
    df = df.withColumn(
        "estado_ocupacion",
        F.when(
            ((F.col("tip_unidad") == "UCI") &
             (F.col("tasa_ocupacion") >= 0.85)),
            F.lit("Critico")
        ).when(
            ((F.col("tip_unidad") == "Urgencias") &
             (F.col("tasa_ocupacion") >= 0.90)),
            F.lit("Critico")
        ).when(
            (F.col("tip_unidad").isin(["General","Cirugia"])) &
            (F.col("tasa_ocupacion") >= 0.88),
            F.lit("Critico")
        ).when(F.col("tasa_ocupacion") >= 0.70, F.lit("Precaucion"))
         .otherwise(F.lit("Normal"))
    )

    # Estandarizar tip_unidad
    df = df.withColumn(
        "tip_unidad",
        F.when(F.col("tip_unidad") == "General",   F.lit("General"))
         .when(F.col("tip_unidad") == "UCI",        F.lit("UCI"))
         .when(F.col("tip_unidad") == "Cirugia",    F.lit("Cirugia"))
         .when(F.col("tip_unidad") == "Urgencias",  F.lit("Urgencias"))
         .otherwise(F.lit("Otro"))
    )

    # Validar FK id_sede
    try:
        df_sedes = spark.read.format("delta") \
                        .load(f"{SILVER_BASE}/RED_SEDES").select("id_sede")
        df, n_fk = validar_integridad_referencial(
            df, df_sedes, "id_sede", "id_sede", tabla, "RED_SEDES"
        )
        n_rechazados += n_fk
    except Exception as e:
        print(f"  ⚠️  FK id_sede no validada: {str(e)[:80]}")

    return df, n_rechazados



# COMMAND ----------


REGLAS_CALIDAD = [
    {
        "nombre":    "id_registro_cama no nulo",
        "condicion": F.col("id_registro_cama").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "num_camas_disp >= 0",
        "condicion": F.col("num_camas_disp") >= 0,
        "critica":   True
    },
    {
        "nombre":    "tasa_ocupacion entre 0 y 1",
        "condicion": F.col("tasa_ocupacion").between(0, 1),
        "critica":   True
    },
    {
        "nombre":    "tip_unidad en catálogo",
        "condicion": F.col("tip_unidad").isin(
            ["General","UCI","Cirugia","Urgencias","Otro"]
        ),
        "critica": False
    },
    {
        "nombre":    "estado_ocupacion asignado",
        "condicion": F.col("estado_ocupacion").isin(
            ["Critico","Precaucion","Normal"]
        ),
        "critica": False
    },
]

procesar_silver(
    tabla              = "GCM_CAMAS",
    pk_cols            = ["id_registro_cama"],
    partition_col      = "fec_hora_registro",
    estrategias_nulos  = {
        "num_camas_mant":         ("imputar",   0),
        "motivo_indisponibilidad":("imputar",   "Sin Motivo"),
    },
    reglas_calidad      = REGLAS_CALIDAD,
    transformaciones_fn = transformar_gcm_camas
)