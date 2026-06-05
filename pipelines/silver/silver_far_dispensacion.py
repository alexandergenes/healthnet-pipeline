# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/03_silver/silver_utils

# COMMAND ----------

from pyspark.sql import functions as F

def transformar_far_dispensacion(df, tabla):
    """Transformaciones específicas de FAR_DISPENSACION."""
    n_rechazados = 0

    # Calcular valor total dispensación
    df = df.withColumn(
        "vr_total_dispensacion",
        F.when(
            F.col("vr_unitario").isNotNull() & F.col("cantidad").isNotNull(),
            F.round(F.col("vr_unitario") * F.col("cantidad"), 2)
        ).otherwise(F.lit(None).cast("double"))
    )

    # Estandarizar tip_prescripcion
    df = df.withColumn(
        "tip_prescripcion",
        F.when(F.col("tip_prescripcion") == "Formulado",    F.lit("Formulado"))
         .when(F.col("tip_prescripcion") == "Venta Libre",  F.lit("Venta Libre"))
         .when(F.col("tip_prescripcion") == "Muestra Medica",F.lit("Muestra Medica"))
         .otherwise(F.lit("No Informado"))
    )

    # Excluir registros con cantidad <= 0
    n_antes = df.count()
    df_inv  = df.filter(F.col("cantidad") <= 0)
    n_inv   = df_inv.count()
    if n_inv > 0:
        df_err = df_inv \
            .withColumn("motivo_error", F.lit("CANTIDAD_INVALIDA: cantidad <= 0")) \
            .withColumn("tabla_origen", F.lit(tabla)) \
            .withColumn("ts_error",     F.lit(datetime.now()).cast("timestamp"))
        registrar_error(df_err, tabla, "CANTIDAD_INVALIDA")
        df = df.filter(F.col("cantidad") > 0)
        n_rechazados += n_inv
        print(f"  ⚠️  Registros con cantidad inválida excluidos: {n_inv:,}")

    # Validar FK pac_id
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
        "nombre":    "id_dispensacion no nulo",
        "condicion": F.col("id_dispensacion").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "cantidad > 0",
        "condicion": F.col("cantidad") > 0,
        "critica":   True
    },
    {
        "nombre":    "cod_medicamento no nulo",
        "condicion": F.col("cod_medicamento").isNotNull(),
        "critica":   True
    },
    {
        "nombre":    "tip_prescripcion en catálogo",
        "condicion": F.col("tip_prescripcion").isin([
            "Formulado","Venta Libre","Muestra Medica","No Informado"
        ]),
        "critica": False
    },
    {
        "nombre":    "pac_id no nulo",
        "condicion": F.col("pac_id").isNotNull(),
        "critica":   True
    },
]

procesar_silver(
    tabla              = "FAR_DISPENSACION",
    pk_cols            = ["id_dispensacion"],
    partition_col      = "fec_dispensacion",
    estrategias_nulos  = {
        "vr_unitario":    ("indicador", None),
        "tip_prescripcion":("imputar",  "No Informado"),
        "id_encuentro":   ("indicador", None),
    },
    reglas_calidad      = REGLAS_CALIDAD,
    transformaciones_fn = transformar_far_dispensacion
)