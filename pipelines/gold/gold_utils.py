# Databricks notebook source
# Configuración

from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

STORAGE_ACCOUNT = "dlshealthnetdev"
SECRET_SCOPE    = "healthnet-kv-scope"

spark.conf.set(
    f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.msi.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    "http://169.254.169.254/oauth2/token"
)

BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"
GOLD_BASE   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"

CDF_VERSIONS_PATH = f"{BRONZE_BASE}/_control/cdf_versions"
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

print("✅ Gold utils configurado")
print(f"   Silver: {SILVER_BASE}")
print(f"   Gold:   {GOLD_BASE}")

# COMMAND ----------

def leer_silver(tabla: str) -> DataFrame:
    """Lee tabla Delta desde Silver."""
    path = f"{SILVER_BASE}/{tabla}"
    df   = spark.read.format("delta").load(path)
    n    = df.count()
    print(f"  📥 Silver {tabla}: {n:,} registros")
    return df


# COMMAND ----------

def leer_silver_cdf(tabla: str):
    """Lee solo cambios de Silver desde última versión procesada via CDF."""
    tabla_uc       = f"dbw_healthnet_dev.default.silver_{tabla.lower()}"
    ultima_version = get_ultima_version_cdf(tabla, "gold")

    try:
        historia       = spark.sql(f"DESCRIBE HISTORY {tabla_uc}")
        version_actual = int(historia.agg(F.max("version")).collect()[0][0])
        cdf_version    = historia.filter(
            F.col("operationParameters").cast("string")
             .contains("enableChangeDataFeed")
        ).agg(F.min("version")).collect()[0][0]
        cdf_start = int(cdf_version) if cdf_version is not None else 0
    except Exception as e:
        print(f"  ⚠️  {tabla_uc} no encontrada: {str(e)[:80]}")
        return None, 0

    if ultima_version >= version_actual:
        print(f"  ℹ️  Sin cambios en silver/{tabla} (v{version_actual})")
        return None, version_actual

    start_version = max(ultima_version + 1, cdf_start)
    print(f"  📖 silver/{tabla}: versiones {start_version} → {version_actual}")

    df = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", start_version)
               .table(tabla_uc)
               .filter(F.col("_change_type").isin(["insert","update_postimage"]))
               .drop("_change_type","_commit_version","_commit_timestamp"))

    audit_cols = ["_silver_ts","_silver_lote","_anio","_mes","_dia"]
    df = df.drop(*[c for c in audit_cols if c in df.columns])

    n = df.count()
    print(f"  ✅ {n:,} cambios detectados en silver/{tabla}")
    return df, version_actual

def get_ultima_version_cdf(tabla: str, capa: str) -> int:
    try:
        df = spark.read.format("delta").load(CDF_VERSIONS_PATH)
        row = df.filter(
            (F.col("tabla") == tabla) & (F.col("capa") == capa)
        ).collect()
        if row:
            return int(row[0]["ultima_version"])
        else:
            return -1  # ← nunca procesado → leer desde v0
    except:
        return -1  # ← tabla cdf_versions no existe → leer desde v0

def update_version_cdf(tabla: str, capa: str, nueva_version: int):
    try:
        df_nueva = spark.createDataFrame(
            [(tabla, capa, nueva_version, datetime.now())],
            ["tabla","capa","ultima_version","ts_actualizacion"]
        ).withColumn("ts_actualizacion", F.col("ts_actualizacion").cast("timestamp"))

        dt = DeltaTable.forPath(spark, CDF_VERSIONS_PATH)
        dt.alias("target").merge(
            df_nueva.alias("source"),
            "(target.tabla = source.tabla AND target.capa = source.capa)"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    except:
        # Tabla cdf_versions no existe — crear con primer registro
        spark.createDataFrame(
            [(tabla, capa, nueva_version, datetime.now())],
            ["tabla","capa","ultima_version","ts_actualizacion"]
        ).withColumn("ts_actualizacion", F.col("ts_actualizacion").cast("timestamp")) \
         .write.format("delta").mode("append").save(CDF_VERSIONS_PATH)

    print(f"  📌 CDF version guardada: {capa}/{tabla} → v{nueva_version}")

def get_version_actual_delta(path: str) -> int:
    historia = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    return int(historia.agg(F.max("version")).collect()[0][0])

# COMMAND ----------

def leer_gold_cdf(tabla: str):
    """Lee solo cambios de Gold desde última versión procesada via CDF."""
    tabla_uc       = f"dbw_healthnet_dev.default.gold_{tabla.lower()}"
    ultima_version = get_ultima_version_cdf(tabla, "gold_kpis")

    try:
        historia       = spark.sql(f"DESCRIBE HISTORY {tabla_uc}")
        version_actual = int(historia.agg(F.max("version")).collect()[0][0])
        cdf_version    = historia.filter(
            F.col("operationParameters").cast("string")
             .contains("enableChangeDataFeed")
        ).agg(F.min("version")).collect()[0][0]
        cdf_start = int(cdf_version) if cdf_version is not None else 0
    except Exception as e:
        print(f"  ⚠️  {tabla_uc} no encontrada: {str(e)[:80]}")
        return None, 0

    if ultima_version >= version_actual:
        print(f"  ℹ️  Sin cambios en gold/{tabla} (v{version_actual})")
        return None, version_actual

    start_version = max(ultima_version + 1, cdf_start)
    print(f"  📖 gold/{tabla}: versiones {start_version} → {version_actual}")

    df = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", start_version)
               .table(tabla_uc)
               .filter(F.col("_change_type").isin(["insert","update_postimage"]))
               .drop("_change_type","_commit_version","_commit_timestamp"))

    audit_cols = ["_gold_ts","_gold_lote"]
    df = df.drop(*[c for c in audit_cols if c in df.columns])

    n = df.count()
    print(f"  ✅ {n:,} cambios detectados en gold/{tabla}")
    return df, version_actual

# COMMAND ----------


def escribir_gold(
    df: DataFrame,
    tabla: str,
    pk_cols: list,
    partition_cols: list,
    lote_id: str
) -> int:
    """
    Escribe en Gold usando MERGE para garantizar idempotencia.
    En primera creación habilita CDF desde v0 y registra en Unity Catalog.
    """
    path = f"{GOLD_BASE}/{tabla}"
    ts   = datetime.now()

    df_final = df \
        .withColumn("_gold_ts",   F.lit(ts).cast("timestamp")) \
        .withColumn("_gold_lote", F.lit(lote_id))

    n = df_final.count()

    try:
        # Tabla ya existe → MERGE idempotente
        dt = DeltaTable.forPath(spark, path)
        merge_condition = " AND ".join(
            [f"target.{c} = source.{c}" for c in pk_cols]
        )
        dt.alias("target").merge(
            df_final.alias("source"),
            merge_condition
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        print(f"  ✅ MERGE Gold {tabla}: {n:,} registros")

    except Exception:
        # Tabla no existe → crear vacía con CDF y partición correcta desde v0
        writer_empty = spark.createDataFrame([], df_final.schema) \
                            .write \
                            .format("delta") \
                            .option("delta.enableChangeDataFeed", "true") \
                            .option("mergeSchema", "true") \
                            .mode("overwrite")
        # Aplicar misma partición que los datos
        if partition_cols:
            writer_empty = writer_empty.partitionBy(*partition_cols)
        writer_empty.save(path)
        print(f"  ✅ Tabla Gold creada con CDF desde v0: {tabla}")

        # Escribir datos
        writer = df_final.write \
                         .format("delta") \
                         .option("mergeSchema", "true") \
                         .mode("append")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        print(f"  ✅ CREATE Gold {tabla}: {n:,} registros")

        # Registrar en Unity Catalog y dar permisos a ADF — solo en creación
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS dbw_healthnet_dev.default.gold_{tabla.lower()}
                USING DELTA LOCATION '{path}'
            """)
            spark.sql(f"""
                GRANT SELECT, MODIFY ON TABLE dbw_healthnet_dev.default.gold_{tabla.lower()}
                TO `307ad978-6b6e-43ed-9b8b-5df1ef8b5d01`
            """)
            print(f"  ✅ Registrada en Unity Catalog: gold_{tabla.lower()}")
        except Exception as e:
            print(f"  ⚠️  Unity Catalog: {str(e)[:80]}")

    return n

# COMMAND ----------

LOG_PATH = f"{GOLD_BASE}/_control/pipeline_log"

def log_gold(tabla: str, lote_id: str, registros: int, estado: str, duracion: int):
    """Registra ejecución Gold en tabla de log."""
    df_log = spark.createDataFrame(
        [(tabla, lote_id, datetime.now(), registros, estado, duracion)],
        ["tabla", "lote_id", "ts_ejecucion", "registros", "estado", "duracion_seg"]
    ).withColumn("ts_ejecucion", F.col("ts_ejecucion").cast("timestamp"))
    df_log.write.format("delta").mode("append").save(LOG_PATH)
    print(f"  📋 Log Gold: {tabla} | {estado} | {registros:,} registros | {duracion}s")

print("✅ gold_utils cargado correctamente")