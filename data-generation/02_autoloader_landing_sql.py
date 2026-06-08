# Databricks notebook source
# HealthNet — AutoLoader: landing/ → Azure SQL Database
# Reemplaza el Event Trigger de ADF por Structured Streaming

# Configuración

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

STORAGE_ACCOUNT = "dlshealthnetdev"
SECRET_SCOPE    = "healthnet-kv-scope"

access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-access-key")
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    access_key
)

# Paths
LANDING_BASE    = f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet/raw"
CHECKPOINT_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet/_autoloader_checkpoints"

# Azure SQL
sql_server   = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-server")
sql_user     = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-user")
sql_password = dbutils.secrets.get(scope=SECRET_SCOPE, key="sql-password")

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

print("✅ AutoLoader configurado")
print(f"   Landing:     {LANDING_BASE}")
print(f"   Checkpoint:  {CHECKPOINT_BASE}")
print(f"   SQL Server:  {sql_server}")

# COMMAND ----------

# Schemas explícitos por tabla

SCHEMAS = {
    "RED_SEDES": StructType([
        StructField("id_sede",            LongType(),    False),
        StructField("nom_sede",           StringType(),  False),
        StructField("tip_sede",           StringType(),  False),
        StructField("id_ciudad",          LongType(),    False),
        StructField("nom_ciudad",         StringType(),  False),
        StructField("id_pais",            LongType(),    False),
        StructField("nom_pais",           StringType(),  False),
        StructField("nivel_complejidad",  LongType(),    False),
        StructField("cap_camas_gen",      LongType(),    False),
        StructField("cap_camas_uci",      LongType(),    False),
        StructField("cap_camas_cirugia",  LongType(),    False),
        StructField("cap_camas_urg",      LongType(),    False),
        StructField("activa",             BooleanType(), False),
        StructField("fec_apertura",       StringType(),  True),
    ]),
    "MED_PLANTA": StructType([
        StructField("med_id",         LongType(),    False),
        StructField("esp_principal",  StringType(),  False),
        StructField("esp_secundaria", StringType(),  True),
        StructField("id_sede",        LongType(),    False),
        StructField("fec_ingreso",    StringType(),  False),
        StructField("tip_contrato",   StringType(),  False),
        StructField("jornada",        StringType(),  False),
        StructField("estado_activo",  BooleanType(), False),
    ]),
    "PAC_REGISTRO": StructType([
        StructField("pac_id",               LongType(),    False),
        StructField("tip_doc",              StringType(),  False),
        StructField("num_doc_hash",         StringType(),  False),
        StructField("fec_nac",              StringType(),  False),
        StructField("genero",               StringType(),  False),
        StructField("id_ciudad_res",        LongType(),    True),
        StructField("nom_ciudad_res",       StringType(),  True),
        StructField("tip_aseguradora",      StringType(),  True),
        StructField("id_eps",               StringType(),  True),
        StructField("estrato_socioec",      LongType(),    True),
        StructField("fec_primer_atencion",  StringType(),  True),
        StructField("activo",               BooleanType(), False),
    ]),
    "AGE_CITAS": StructType([
        StructField("id_cita",               LongType(),    False),
        StructField("pac_id",                LongType(),    False),
        StructField("med_id",                LongType(),    False),
        StructField("id_sede",               LongType(),    False),
        StructField("fec_agendamiento",      StringType(),  False),
        StructField("fec_cita_programada",   StringType(),  False),
        StructField("hra_cita_programada",   StringType(),  False),
        StructField("hra_llegada_paciente",  StringType(),  True),
        StructField("hra_inicio_atencion",   StringType(),  True),
        StructField("esp_solicitada",        StringType(),  False),
        StructField("tip_cita",              StringType(),  False),
        StructField("estado_cita",           StringType(),  False),
    ]),
    "HCE_ENCUENTROS": StructType([
        StructField("id_encuentro",          LongType(),    False),
        StructField("pac_id",                LongType(),    False),
        StructField("med_id",                LongType(),    False),
        StructField("id_sede",               LongType(),    False),
        StructField("fec_registro",          StringType(),  False),
        StructField("fec_inicio_atencion",   StringType(),  True),
        StructField("fec_egreso",            StringType(),  True),
        StructField("tip_consulta",          StringType(),  False),
        StructField("esp_atendida",          StringType(),  False),
        StructField("diag_principal_cie10",  StringType(),  False),
        StructField("diag_sec1_cie10",       StringType(),  True),
        StructField("cod_procedimientos",    StringType(),  True),
        StructField("vr_facturado",          DoubleType(),  True),
        StructField("estado_factura",        StringType(),  True),
    ]),
    "GCM_CAMAS": StructType([
        StructField("id_registro_cama",        LongType(),    False),
        StructField("id_sede",                 LongType(),    False),
        StructField("tip_unidad",              StringType(),  False),
        StructField("fec_hora_registro",       StringType(),  False),
        StructField("num_camas_ocupadas",      LongType(),    False),
        StructField("num_camas_disp",          LongType(),    False),
        StructField("num_camas_mant",          LongType(),    True),
        StructField("motivo_indisponibilidad", StringType(),  True),
    ]),
    "FAR_DISPENSACION": StructType([
        StructField("id_dispensacion",  LongType(),    False),
        StructField("id_encuentro",     LongType(),    True),
        StructField("pac_id",           LongType(),    False),
        StructField("id_sede",          LongType(),    False),
        StructField("fec_dispensacion", StringType(),  False),
        StructField("cod_medicamento",  StringType(),  False),
        StructField("nom_medicamento",  StringType(),  False),
        StructField("cantidad",         LongType(),    False),
        StructField("vr_unitario",      DoubleType(),  True),
        StructField("tip_prescripcion", StringType(),  True),
    ]),
}

# Formato de cada tabla en landing/
FORMATO_TABLA = {
    "RED_SEDES":        "csv",
    "MED_PLANTA":       "csv",
    "PAC_REGISTRO":     "parquet",
    "AGE_CITAS":        "parquet",
    "HCE_ENCUENTROS":   "parquet",
    "GCM_CAMAS":        "json",
    "FAR_DISPENSACION": "json",
}

# Orden de carga respetando integridad referencial
ORDEN_CARGA = [
    "RED_SEDES",
    "MED_PLANTA",
    "PAC_REGISTRO",
    "AGE_CITAS",
    "HCE_ENCUENTROS",
    "GCM_CAMAS",
    "FAR_DISPENSACION",
]

print("✅ Schemas y configuración de tablas listos")

# COMMAND ----------

# Función de carga a SQL (micro-batch)

def cargar_batch_a_sql(df, batch_id, tabla):
    from pyspark.sql import functions as F

    if df.count() == 0:
        print(f"  Batch {batch_id} vacío para {tabla} — omitiendo")
        return

    # Eliminar columna _rescued_data que AutoLoader agrega automáticamente
    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")

    # Castear tipos al schema correcto
    schema = SCHEMAS[tabla]
    for field in schema.fields:
        if field.name in df.columns:
            df = df.withColumn(
                field.name,
                F.col(field.name).cast(field.dataType)
            )

    # Agregar después del cast general, antes del .save()
    if "vr_facturado" in df.columns:
        df = df.withColumn("vr_facturado", 
                        F.round(F.col("vr_facturado").cast(DecimalType(14,2)), 2))

    # Agregar junto al fix de vr_facturado
    if "vr_unitario" in df.columns:
        df = df.withColumn("vr_unitario",
                        F.round(F.col("vr_unitario").cast(DecimalType(10,2)), 2))

    # Agregar fec_modificacion — timestamp de cuando se escribe en SQL
    df = df.withColumn("fec_modificacion", F.current_timestamp())        

    n = df.count()
    (df.write
       .format("jdbc")
       .option("url", JDBC_URL)
       .option("dbtable", f"dbo.{tabla}")
       .option("batchsize", 10000)
       .options(**JDBC_PROPS)
       .mode("append")
       .save())

    print(f"  ✅ Batch {batch_id} | {tabla} | {n:,} registros → Azure SQL")

# COMMAND ----------

# AutoLoader por tabla

def iniciar_autoloader(tabla: str, trigger_once: bool = True):
    formato   = FORMATO_TABLA[tabla]
    path_in   = f"{LANDING_PATH}/{tabla}"
    ckpt_path = f"{CHECKPOINT_BASE}/{tabla}"

    print(f"\n  🔄 AutoLoader: {tabla} | formato={formato}")

    # Leer SIN schema forzado — dejar que AutoLoader infiera
    reader = (spark.readStream
                   .format("cloudFiles")
                   .option("cloudFiles.format", formato)
                   .option("cloudFiles.schemaLocation", f"{ckpt_path}/schema")
                   .option("cloudFiles.inferColumnTypes", "true"))

    if formato == "csv":
        reader = reader.option("header", "true").option("nullValue", "")

    # Cargar sin schema explícito
    df_stream = reader.load(path_in)

    def batch_fn(df, batch_id):
        try:
            cargar_batch_a_sql(df, batch_id, tabla)
        except Exception as e:
            import traceback
            print(f"  ❌ Error detallado batch {batch_id}:")
            print(traceback.format_exc())
            raise

    writer = (df_stream.writeStream
                       .foreachBatch(batch_fn)
                       .option("checkpointLocation", f"{ckpt_path}/checkpoint")
                       .queryName(f"autoloader_{tabla}"))

    if trigger_once:
        query = writer.trigger(once=True).start()
        query.awaitTermination()
        print(f"  ✅ AutoLoader completado: {tabla}")
    else:
        return writer.trigger(processingTime="5 minutes").start()

    def batch_fn(df, batch_id):
        try:
            cargar_batch_a_sql(df, batch_id, tabla)
        except Exception as e:
            # Mostrar error completo
            import traceback
            print(f"  ❌ Error detallado batch {batch_id}:")
            print(traceback.format_exc())
            raise

    writer = (df_stream.writeStream
                       .foreachBatch(batch_fn)
                       .option("checkpointLocation", f"{ckpt_path}/checkpoint")
                       .queryName(f"autoloader_{tabla}"))

    if trigger_once:
        query = writer.trigger(once=True).start()
        query.awaitTermination()
        print(f"  ✅ AutoLoader completado: {tabla}")
    else:
        return writer.trigger(processingTime="5 minutes").start()

    # Configurar AutoLoader según formato
    reader = (spark.readStream
                   .format("cloudFiles")
                   .option("cloudFiles.format", formato)
                   .option("cloudFiles.schemaLocation", f"{ckpt_path}/schema")
                   .option("cloudFiles.inferColumnTypes", "false"))

    if formato == "csv":
        reader = reader.option("header", "true") \
                       .option("nullValue", "")
    elif formato == "parquet":
        reader = reader.option("mergeSchema", "false")
    elif formato == "json":
        reader = reader.option("multiLine", "false")

    df_stream = reader.load(path_in)

    # Configurar writer con foreachBatch
    writer = (df_stream.writeStream
                       .foreachBatch(lambda df, bid: cargar_batch_a_sql(df, bid, tabla))
                       .option("checkpointLocation", f"{ckpt_path}/checkpoint")
                       .queryName(f"autoloader_{tabla}"))

    if trigger_once:
        # Procesa archivos pendientes y termina — para ADF schedule
        query = writer.trigger(once=True).start()
        query.awaitTermination()
        print(f"  ✅ AutoLoader completado: {tabla}")
    else:
        # Streaming continuo
        query = writer.trigger(processingTime="5 minutes").start()
        return query



# COMMAND ----------

# Orquestación — procesar todas las tablas

import traceback

inicio = datetime.now()

print("=" * 60)
print("  HealthNet — AutoLoader: landing/ → Azure SQL")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

LANDING_PATH = LANDING_BASE
errores = []

for tabla in ORDEN_CARGA:
    try:
        # Limpiar checkpoint anterior si existe
        ckpt_path = f"{CHECKPOINT_BASE}/{tabla}"
        try:
            dbutils.fs.rm(f"{ckpt_path}/checkpoint", recurse=True)
            print(f"  🧹 Checkpoint limpiado: {tabla}")
        except:
            pass

        iniciar_autoloader(tabla, trigger_once=True)

    except Exception as e:
        error_completo = traceback.format_exc()
        print(f"\n  ❌ Error en {tabla}:")
        print(error_completo[:1000])
        errores.append({"tabla": tabla, "error": str(e)[:200]})
        # Continuar con la siguiente tabla
        continue

# COMMAND ----------

# Evidencia — COUNT(*) desde Azure SQL

duracion = (datetime.now() - inicio).seconds

print("\n" + "=" * 65)
print("  EVIDENCIA — COUNT(*) DESDE AZURE SQL POST AUTOLOADER")
print("=" * 65)
print(f"{'TABLA':<25} {'FILAS SQL':>12}  {'ESTADO':>10}")
print("-" * 52)

total = 0
for tabla in ORDEN_CARGA:
    try:
        query = f"(SELECT COUNT(1) AS cnt FROM dbo.{tabla}) t"
        n = (spark.read.format("jdbc")
                  .option("url", JDBC_URL)
                  .option("dbtable", query)
                  .options(**JDBC_PROPS)
                  .load()
                  .collect()[0]["cnt"])
        estado = "❌ ERROR" if any(e["tabla"] == tabla for e in errores) else "✅ OK"
        total += n
        print(f"{tabla:<25} {n:>12,}  {estado:>10}")
    except Exception as e:
        print(f"{tabla:<25} {'ERROR':>12}  {'❌':>10}")

print("-" * 52)
print(f"{'TOTAL':<25} {total:>12,}")
print(f"\n⏱  Duración: {duracion} segundos")

if errores:
    print(f"\n⚠️  {len(errores)} tablas con errores:")
    for e in errores:
        print(f"   {e['tabla']}: {e['error']}")
else:
    print(f"\n✅ AutoLoader completado exitosamente")