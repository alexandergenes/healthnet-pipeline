# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/02_bronze/bronze_utils

# COMMAND ----------

ingestar_bronze(
    tabla         = "RED_SEDES",
    watermark_col = "fec_modificacion",    # ← watermark por fecha de carga
    partition_col = "fec_apertura",
    estrategia    = "full_load"
)
 