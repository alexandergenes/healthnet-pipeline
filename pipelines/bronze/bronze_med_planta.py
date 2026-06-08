# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/02_bronze/bronze_utils

# COMMAND ----------

ingestar_bronze(
    tabla         = "MED_PLANTA",
    watermark_col = "fec_modificacion",
    partition_col = "fec_ingreso",
    estrategia    = "incremental"
)