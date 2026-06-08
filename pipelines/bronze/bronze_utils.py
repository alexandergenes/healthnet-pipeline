# Databricks notebook source
#   Notebook de utilidades compartidas por todos los notebooks
#   de Bronze. Gestiona la tabla de watermarks, conexión JDBC,
#   escritura Delta y logging de ejecuciones.
#   Importar con: %run ./bronze_utils

# Configuración de acceso

from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

STORAGE_ACCOUNT = "dlshealthnetdev"
SECRET_SCOPE    = "healthnet-kv-scope"

# Acceso ADLS Gen2
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

# Credenciales Azure SQL
sql_server   = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-server")
sql_user     = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-user")
sql_password = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-password")

# Paths base
BRONZE_BASE  = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"
LANDING_BASE = f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"

# JDBC
JDBC_URL = (
    f"jdbc:sqlserver://{sql_server}:1433;"
    f"database=healthnet-source;"
    f"encrypt=true;trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)
JDBC_PROPS = {
    "user":     sql_user,
    "password": sql_password,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

print("✅ Configuración Bronze cargada")
print(f"   Bronze: {BRONZE_BASE}")
print(f"   SQL:    {sql_server}")

# COMMAND ----------

# Crear tabla de watermarks si no existe

WATERMARK_PATH = f"{BRONZE_BASE}/_control/pipeline_watermark"

def init_watermark_table():
    """Crea la tabla Delta de watermarks si no existe."""
    try:
        spark.read.format("delta").load(WATERMARK_PATH)
        print("✅ Tabla watermark existente cargada")
    except Exception:
        # Crear tabla watermark inicial
        watermark_data = [
            ("RED_SEDES",        "incremental","1900-01-01 00:00:00", "1900-01-01 00:00:00"),
            ("MED_PLANTA",       "incremental","1900-01-01 00:00:00", "1900-01-01 00:00:00"),
            ("PAC_REGISTRO",     "incremental","1900-01-01 00:00:00", "1900-01-01 00:00:00"),
            ("AGE_CITAS",        "incremental","1900-01-01 00:00:00", "1900-01-01 00:00:00"),
            ("HCE_ENCUENTROS",   "incremental","1900-01-01 00:00:00", "1900-01-01 00:00:00"),
            ("GCM_CAMAS",        "incremental","1900-01-01 00:00:00", "1900-01-01 00:00:00"),
            ("FAR_DISPENSACION", "incremental","1900-01-01 00:00:00", "1900-01-01 00:00:00"),
        ]
        df_wm = spark.createDataFrame(
            watermark_data,
            ["tabla_nombre", "estrategia", "ultimo_proceso", "ultima_ejecucion"]
        ).withColumn("ultimo_proceso",   F.col("ultimo_proceso").cast("timestamp")) \
         .withColumn("ultima_ejecucion", F.col("ultima_ejecucion").cast("timestamp"))

        df_wm.write.format("delta").mode("overwrite").save(WATERMARK_PATH)
        print("✅ Tabla watermark creada")

init_watermark_table()


# COMMAND ----------

# Funciones de watermark

def get_watermark(tabla: str) -> datetime:
    """Lee el último watermark procesado para una tabla."""
    df = spark.read.format("delta").load(WATERMARK_PATH)
    row = df.filter(F.col("tabla_nombre") == tabla).collect()
    if row:
        return row[0]["ultimo_proceso"]
    return datetime(1900, 1, 1)

def update_watermark(tabla: str, nuevo_ts: datetime):
    """Actualiza el watermark de una tabla después de procesar."""
    
    # Truncar microsegundos para evitar re-procesamiento en siguiente ejecución
    nuevo_ts = nuevo_ts.replace(microsecond=0) + timedelta(seconds=1)
    
    dt_wm = DeltaTable.forPath(spark, WATERMARK_PATH)
    dt_wm.update(
        condition = F.col("tabla_nombre") == tabla,
        set = {
            "ultimo_proceso":   F.lit(nuevo_ts).cast("timestamp"),
            "ultima_ejecucion": F.lit(datetime.now()).cast("timestamp")
        }
    )
    print(f"  ✅ Watermark actualizado: {tabla} → {nuevo_ts}")


# COMMAND ----------

# Función de lectura desde SQL

def leer_desde_sql(
    tabla: str,
    watermark_col: str = None,
    watermark_val: datetime = None,
    schema: StructType = None
) -> DataFrame:
    if watermark_col and watermark_val:
        # Truncar microsegundos para compatibilidad con Azure SQL
        wm_str = str(watermark_val)[:19]  # "2026-06-06 03:15:00"
        query = f"""(
            SELECT *
            FROM dbo.{tabla}
            WHERE CAST({watermark_col} AS DATETIME) > '{wm_str}'
        ) t"""
        print(f"  Modo: INCREMENTAL | {watermark_col} > {wm_str}")
    else:
        query = f"(SELECT * FROM dbo.{tabla}) t"
        print(f"  Modo: FULL LOAD")

    reader = spark.read.format("jdbc") \
                  .option("url", JDBC_URL) \
                  .option("dbtable", query) \
                  .options(**JDBC_PROPS)

    if schema:
        reader = reader.schema(schema)

    df = reader.load()
    n  = df.count()
    print(f"  Registros leídos: {n:,}")
    return df


# COMMAND ----------

# Función de escritura Delta en Bronze

def escribir_bronze(
    df: DataFrame,
    tabla: str,
    partition_col: str,
    lote_id: str,
    modo: str = "incremental"
) -> int:
    ts_ingesta = datetime.now()
    path = f"{BRONZE_BASE}/{tabla}"

    df_audit = df \
        .withColumn("_ingesta_ts", F.lit(ts_ingesta).cast("timestamp")) \
        .withColumn("_fuente",     F.lit("Azure_SQL_healthnet-source")) \
        .withColumn("_lote_id",    F.lit(lote_id)) \
        .withColumn("_anio",       F.year(F.col(partition_col).cast("timestamp"))) \
        .withColumn("_mes",        F.month(F.col(partition_col).cast("timestamp"))) \
        .withColumn("_dia",        F.dayofmonth(F.col(partition_col).cast("timestamp")))

    n = df_audit.count()

    if n == 0:
        print(f"  ⚠️  Sin registros nuevos para {tabla} — omitiendo escritura")
        return 0

    # Crear tabla vacía con CDF habilitado ANTES de escribir datos
    try:
        DeltaTable.forPath(spark, path)
        # Tabla ya existe — solo asegurar CDF habilitado
        spark.sql(f"""
            ALTER TABLE delta.`{path}`
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
    except:
        # Tabla no existe → crear vacía con CDF desde v0
        spark.createDataFrame([], df_audit.schema) \
             .write \
             .format("delta") \
             .option("delta.enableChangeDataFeed", "true") \
             .partitionBy("_anio", "_mes", "_dia") \
             .mode("overwrite") \
             .save(path)
        print(f"  ✅ Tabla Bronze creada con CDF desde v0: {tabla}")

    # Ahora escribir datos — CDF ya habilitado
    writer = df_audit.write \
                     .format("delta") \
                     .option("mergeSchema", "true") \
                     .partitionBy("_anio", "_mes", "_dia")

    if modo == "full_load":
        writer.mode("overwrite").save(path)
        print(f"  ✅ Full load escrito en Bronze: {n:,} registros")
    else:
        writer.mode("append").save(path)
        print(f"  ✅ Incremental escrito en Bronze: {n:,} registros")

    print(f"  ✅ CDF activo en Bronze: {tabla}")
    return n


# COMMAND ----------

# Función de logging de ejecuciones

LOG_PATH = f"{BRONZE_BASE}/_control/pipeline_log"

def log_ejecucion(
    tabla: str,
    lote_id: str,
    registros: int,
    estado: str,
    mensaje: str = "",
    duracion_seg: int = 0
):
    """Registra resultado de cada ejecución incluyendo tamaño y duración."""
    import subprocess

    # Calcular tamaño del archivo Delta en ADLS
    try:
        path = f"{BRONZE_BASE}/{tabla}"
        files = dbutils.fs.ls(path)
        tamano_mb = round(
            sum(f.size for f in dbutils.fs.ls(path) if f.size > 0) / (1024 * 1024), 2
        )
    except:
        tamano_mb = 0.0

    log_data = [(
        tabla,
        lote_id,
        datetime.now(),
        registros,
        tamano_mb,
        duracion_seg,
        estado,
        mensaje[:500] if mensaje else ""
    )]

    df_log = spark.createDataFrame(
        log_data,
        ["tabla", "lote_id", "ts_ejecucion", "registros_procesados",
         "tamano_mb", "duracion_seg", "estado", "mensaje"]
    ).withColumn("ts_ejecucion", F.col("ts_ejecucion").cast("timestamp"))

    df_log.write.format("delta").mode("append").save(LOG_PATH)
    print(f"  📋 Log: {tabla} | {estado} | {registros:,} registros | {tamano_mb} MB | {duracion_seg}s")

# COMMAND ----------

# Función principal de ingesta Bronze

def ingestar_bronze(
    tabla: str,
    watermark_col: str,
    partition_col: str,
    estrategia: str = "incremental",
    schema: StructType = None
):
    """
    Función principal que orquesta la ingesta completa de una tabla.
    Maneja errores, watermarks y logging automáticamente.
    """
    lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("=" * 60)
    print(f"  Bronze: {tabla}")
    print(f"  Lote:   {lote_id}")
    print(f"  Modo:   {estrategia.upper()}")
    print("=" * 60)

    try:
        # 1. Obtener watermark
        if estrategia == "incremental":
            wm = get_watermark(tabla)
        else:
            wm = None

        # 2. Leer desde SQL
        df = leer_desde_sql(
            tabla        = tabla,
            watermark_col = watermark_col if estrategia == "incremental" else None,
            watermark_val = wm,
            schema        = schema
        )

        if df.count() == 0:
            log_ejecucion(tabla, lote_id, 0, "SIN_CAMBIOS", "No hay registros nuevos")
            return

        # 3. Obtener max watermark de los datos leídos
        if estrategia == "incremental" and watermark_col:
            max_wm = df.agg(
                F.max(F.col(watermark_col).cast("timestamp"))
            ).collect()[0][0]
        else:
            max_wm = datetime.now()

        # 4. Escribir en Bronze
        n = escribir_bronze(df, tabla, partition_col, lote_id, estrategia)

        # 5. Actualizar watermark
        if estrategia == "incremental" and max_wm:
            update_watermark(tabla, max_wm)

        # 6. Log exitoso
        log_ejecucion(tabla, lote_id, n, "EXITOSO")

        print(f"\n✅ {tabla} completado | {n:,} registros | Lote: {lote_id}")

    except Exception as e:
        error_msg = str(e)[:500]
        log_ejecucion(tabla, lote_id, 0, "ERROR", error_msg)
        print(f"\n❌ Error en {tabla}: {error_msg}")
        raise

print("✅ bronze_utils cargado correctamente")