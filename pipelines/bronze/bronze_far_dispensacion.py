# Databricks notebook source
# MAGIC %run /Workspace/healthnet-pipeline/02_bronze/bronze_utils

# COMMAND ----------

ingestar_bronze(
    tabla         = "FAR_DISPENSACION",
    watermark_col = "fec_modificacion",
    partition_col = "fec_dispensacion",
    estrategia    = "incremental"
)