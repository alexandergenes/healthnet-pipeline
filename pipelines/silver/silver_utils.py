# Databricks notebook source
# DESCRIPCIÓN:
#   Funciones compartidas para todos los notebooks Silver:
#   - Deduplicación
#   - Manejo de nulos (imputación, exclusión, indicador binario)
#   - Enmascaramiento PII (SHA-256)
#   - Validación de integridad referencial
#   - Tabla de errores del pipeline
#   - Reporte de calidad de datos
#   - Escritura Delta idempotente (MERGE)

# Configuración de acceso

from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable
import hashlib

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
CDF_VERSIONS_PATH = f"{BRONZE_BASE}/_control/cdf_versions"

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

print("✅ Silver utils configurado")
print(f"   Bronze: {BRONZE_BASE}")
print(f"   Silver: {SILVER_BASE}")

# COMMAND ----------

# Paths de control

ERRORES_PATH  = f"{SILVER_BASE}/_control/errores_pipeline"
CALIDAD_PATH  = f"{SILVER_BASE}/_control/reporte_calidad"
LOG_PATH      = f"{SILVER_BASE}/_control/pipeline_log"

# COMMAND ----------

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

def get_version_actual_delta(tabla_uc: str) -> int:
    historia = spark.sql(f"DESCRIBE HISTORY {tabla_uc}")
    return int(historia.agg(F.max("version")).collect()[0][0])

# COMMAND ----------

# Lectura desde Bronze

def leer_bronze(tabla: str):
    """Lee solo cambios de Bronze desde última versión procesada via CDF."""
    tabla_uc       = f"dbw_healthnet_dev.default.bronze_{tabla.lower()}"
    ultima_version = get_ultima_version_cdf(tabla, "silver")

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
        print(f"  ℹ️  Sin cambios en bronze/{tabla} (v{version_actual})")
        return None, version_actual

    start_version = max(ultima_version + 1, cdf_start)
    print(f"  📖 bronze/{tabla}: versiones {start_version} → {version_actual}")

    df = (spark.read.format("delta")
               .option("readChangeFeed", "true")
               .option("startingVersion", start_version)
               .table(tabla_uc)
               .filter(F.col("_change_type").isin(["insert","update_postimage"]))
               .drop("_change_type","_commit_version","_commit_timestamp"))

    audit_cols = ["_ingesta_ts","_fuente","_lote_id","_anio","_mes","_dia"]
    df = df.drop(*[c for c in audit_cols if c in df.columns])

    n = df.count()
    print(f"  ✅ {n:,} cambios detectados")
    return df, version_actual

# COMMAND ----------

# Deduplicación

def deduplicar(df: DataFrame, pk_cols: list, tabla: str) -> tuple:
    """
    Elimina duplicados exactos basándose en las columnas PK.
    Retorna (df_limpio, n_duplicados).
    """
    n_antes  = df.count()
    df_clean = df.dropDuplicates(pk_cols)
    n_duplic = n_antes - df_clean.count()

    if n_duplic > 0:
        print(f"  🔁 Duplicados eliminados en {tabla}: {n_duplic:,}")
        # Registrar duplicados en tabla de errores
        df_dup = df.exceptAll(df_clean) \
                   .withColumn("motivo_error", F.lit("DUPLICADO_EXACTO")) \
                   .withColumn("tabla_origen", F.lit(tabla)) \
                   .withColumn("ts_error",     F.lit(datetime.now()).cast("timestamp"))
        registrar_error(df_dup, tabla, "DUPLICADO")
    else:
        print(f"  ✅ Sin duplicados en {tabla}")

    return df_clean, n_duplic

# COMMAND ----------

# Manejo de nulos

def manejar_nulos(df: DataFrame, estrategias: dict) -> DataFrame:
    """
    Aplica estrategia de nulos por columna.
    estrategias = {
        "columna": ("imputar", valor_default),
        "columna": ("excluir", None),
        "columna": ("indicador", None),
    }
    """
    for col_name, (estrategia, valor) in estrategias.items():
        if col_name not in df.columns:
            continue

        if estrategia == "imputar":
            df = df.fillna({col_name: valor})
            print(f"  📝 Nulos imputados en {col_name} → '{valor}'")

        elif estrategia == "excluir":
            n_antes = df.count()
            df      = df.filter(F.col(col_name).isNotNull())
            n_excl  = n_antes - df.count()
            print(f"  🗑️  Registros excluidos por nulo en {col_name}: {n_excl:,}")

        elif estrategia == "indicador":
            flag_col = f"_nulo_{col_name}"
            df = df.withColumn(
                flag_col,
                F.when(F.col(col_name).isNull(), 1).otherwise(0)
            )
            print(f"  🚩 Indicador binario creado: {flag_col}")

    return df

# COMMAND ----------

# Enmascaramiento PII

# Columnas PII por tabla — enmascarar en Silver
PII_COLS = {
    "PAC_REGISTRO":     [],          # num_doc_hash ya viene hasheado desde origen
    "AGE_CITAS":        [],          # sin PII directo
    "HCE_ENCUENTROS":   ["vr_facturado"],  # valor financiero → enmascarar
    "GCM_CAMAS":        [],
    "FAR_DISPENSACION": ["vr_unitario"],   # valor financiero → enmascarar
    "MED_PLANTA":       [],
    "RED_SEDES":        [],
}

def enmascarar_pii(df: DataFrame, tabla: str) -> DataFrame:
    """
    Enmascara columnas PII usando SHA-256 para strings
    y NULL con flag para valores financieros sensibles.
    """
    cols_pii = PII_COLS.get(tabla, [])

    for col_name in cols_pii:
        if col_name not in df.columns:
            continue
        # Valores financieros → reemplazar por NULL en Silver
        # Solo analistas con acceso a Gold pueden ver agregados
        df = df.withColumn(col_name, F.lit(None).cast(DoubleType())) \
               .withColumn(f"_{col_name}_enmascarado", F.lit(1))
        print(f"  🔒 PII enmascarado: {col_name}")

    return df

# Función SHA-256 para columnas string
sha256_udf = F.udf(
    lambda x: hashlib.sha256(x.encode()).hexdigest() if x else None,
    StringType()
)

# COMMAND ----------

# Validación de integridad referencial 

def validar_integridad_referencial(
    df_hechos: DataFrame,
    df_dim: DataFrame,
    fk_col: str,
    pk_col: str,
    tabla_hechos: str,
    tabla_dim: str
) -> tuple:
    """
    Valida que todos los FK en df_hechos existen en df_dim.
    Los registros huérfanos van a la tabla de errores.
    Retorna (df_valido, n_rechazados).
    """
    # Anti-join: registros en hechos sin match en dimensión
    df_invalidos = df_hechos.join(
        df_dim.select(pk_col).distinct(),
        df_hechos[fk_col] == df_dim[pk_col],
        "left_anti"
    )

    n_invalidos = df_invalidos.count()

    if n_invalidos > 0:
        df_err = df_invalidos \
            .withColumn("motivo_error",
                        F.lit(f"FK_INVALIDA: {fk_col} no existe en {tabla_dim}")) \
            .withColumn("tabla_origen", F.lit(tabla_hechos)) \
            .withColumn("ts_error",     F.lit(datetime.now()).cast("timestamp"))
        registrar_error(df_err, tabla_hechos, "FK_INVALIDA")
        print(f"  ⚠️  Registros con FK inválida ({fk_col}): {n_invalidos:,} → tabla errores")
    else:
        print(f"  ✅ Integridad referencial OK: {fk_col} → {tabla_dim}")

    # Retornar solo registros válidos
    df_valido = df_hechos.join(
    df_dim.select(F.col(pk_col).alias(f"_dim_{pk_col}")).distinct(),
    df_hechos[fk_col] == F.col(f"_dim_{pk_col}"),
    "inner"
    ).drop(f"_dim_{pk_col}")

    return df_valido, n_invalidos

# COMMAND ----------

# Tabla de errores del pipeline

def registrar_error(df_errores: DataFrame, tabla: str, tipo_error: str):
    """
    Registra registros rechazados en la tabla de errores del pipeline.
    Estructura: tabla_origen, motivo_error, ts_error, datos (JSON)
    """
    try:
        # Convertir registro a JSON para almacenar el payload completo
        cols_base = ["tabla_origen", "motivo_error", "ts_error"]
        cols_disponibles = [c for c in cols_base if c in df_errores.columns]

        # Agregar columnas faltantes
        df_err = df_errores
        if "tabla_origen" not in df_err.columns:
            df_err = df_err.withColumn("tabla_origen", F.lit(tabla))
        if "motivo_error" not in df_err.columns:
            df_err = df_err.withColumn("motivo_error", F.lit(tipo_error))
        if "ts_error" not in df_err.columns:
            df_err = df_err.withColumn("ts_error", F.lit(datetime.now()).cast("timestamp"))

        # Serializar payload a JSON
        data_cols = [c for c in df_err.columns
                     if c not in ["tabla_origen","motivo_error","ts_error"]]
        df_final = df_err.select(
            F.col("tabla_origen"),
            F.col("motivo_error"),
            F.col("ts_error"),
            F.to_json(F.struct(*[F.col(c) for c in data_cols])).alias("payload")
        )

        df_final.write.format("delta").mode("append").save(ERRORES_PATH)
        print(f"  📋 {df_final.count():,} errores registrados en tabla errores")

    except Exception as e:
        print(f"  ⚠️  No se pudo registrar error: {str(e)[:100]}")

# COMMAND ----------

# Reporte de calidad de datos

def generar_reporte_calidad(
    df_bronze: DataFrame,
    df_silver: DataFrame,
    tabla: str,
    lote_id: str,
    n_duplicados: int,
    n_rechazados: int
) -> dict:
    """
    Genera reporte de calidad por ejecución:
    - % nulos por columna
    - # registros rechazados
    - % registros conformes
    """
    n_bronze = df_bronze.count()
    n_silver = df_silver.count()
    n_conformes = n_silver
    pct_conformes = round((n_conformes / n_bronze * 100), 2) if n_bronze > 0 else 0

    # % nulos por columna en Silver
    nulos_por_col = {}
    for col_name in df_silver.columns:
        if col_name.startswith("_"):
            continue
        n_nulos = df_silver.filter(F.col(col_name).isNull()).count()
        pct     = round(n_nulos / n_silver * 100, 2) if n_silver > 0 else 0
        nulos_por_col[col_name] = pct

    reporte = {
        "tabla":            tabla,
        "lote_id":          lote_id,
        "ts_reporte":       datetime.now(),
        "n_registros_bronze":  n_bronze,
        "n_registros_silver":  n_silver,
        "n_duplicados":        n_duplicados,
        "n_rechazados":        n_rechazados,
        "pct_conformes":       pct_conformes,
        "detalle_nulos":       str(nulos_por_col),
    }

    # Persistir reporte en Delta
    df_rep = spark.createDataFrame([reporte]) \
                  .withColumn("ts_reporte", F.col("ts_reporte").cast("timestamp"))
    df_rep.write.format("delta").mode("append").save(CALIDAD_PATH)

    # Imprimir resumen
    print(f"\n  📊 REPORTE DE CALIDAD — {tabla}")
    print(f"     Bronze:      {n_bronze:,} registros")
    print(f"     Silver:      {n_silver:,} registros")
    print(f"     Duplicados:  {n_duplicados:,}")
    print(f"     Rechazados:  {n_rechazados:,}")
    print(f"     Conformes:   {pct_conformes}%")
    print(f"     Nulos por columna:")
    for col, pct in nulos_por_col.items():
        if pct > 0:
            print(f"       {col:<30} {pct:>6.1f}%")

    return reporte


# COMMAND ----------

# Escritura Delta idempotente (MERGE)

def escribir_silver(
    df: DataFrame,
    tabla: str,
    pk_cols: list,
    partition_col: str,
    lote_id: str
) -> int:
    path = f"{SILVER_BASE}/{tabla}"
    ts   = datetime.now()

    df_final = df \
        .withColumn("_silver_ts",   F.lit(ts).cast("timestamp")) \
        .withColumn("_silver_lote", F.lit(lote_id)) \
        .withColumn("_anio",        F.year(F.col(partition_col).cast("timestamp"))) \
        .withColumn("_mes",         F.month(F.col(partition_col).cast("timestamp"))) \
        .withColumn("_dia",         F.dayofmonth(F.col(partition_col).cast("timestamp")))

    n = df_final.count()

    try:
        # Tabla existe → MERGE
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
        print(f"  ✅ MERGE Silver {tabla}: {n:,} registros (idempotente)")

    except Exception:
        # Tabla no existe → crear vacía con CDF desde v0
        spark.createDataFrame([], df_final.schema) \
             .write \
             .format("delta") \
             .option("delta.enableChangeDataFeed", "true") \
             .option("mergeSchema", "true") \
             .partitionBy("_anio", "_mes", "_dia") \
             .mode("overwrite") \
             .save(path)
        print(f"  ✅ Tabla Silver creada con CDF desde v0: {tabla}")

        # Escribir datos
        df_final.write \
                .format("delta") \
                .option("mergeSchema", "true") \
                .partitionBy("_anio", "_mes", "_dia") \
                .mode("append") \
                .save(path)
        print(f"  ✅ CREATE Silver {tabla}: {n:,} registros")

    # Registrar en Unity Catalog
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS dbw_healthnet_dev.default.silver_{tabla.lower()}
            USING DELTA LOCATION '{path}'
        """)
        spark.sql(f"""
            GRANT SELECT, MODIFY ON TABLE dbw_healthnet_dev.default.silver_{tabla.lower()}
            TO `307ad978-6b6e-43ed-9b8b-5df1ef8b5d01`
        """)
    except Exception as e:
        print(f"  ⚠️  Unity Catalog: {str(e)[:80]}")

    return n


# COMMAND ----------

# Pruebas de calidad (5 validaciones)

def ejecutar_pruebas_calidad(df: DataFrame, tabla: str, reglas: list) -> dict:
    """
    Ejecuta 5 validaciones de calidad sobre el DataFrame Silver.
    reglas = [
        {"nombre": "...", "condicion": col_expr, "critica": True/False}
    ]
    Retorna dict con resultados de cada prueba.
    """
    resultados = {}
    n_total    = df.count()

    print(f"\n  🧪 PRUEBAS DE CALIDAD — {tabla}")
    print(f"  {'PRUEBA':<40} {'RESULTADO':>10}  {'TASA FALLO':>12}")
    print(f"  {'-'*65}")

    for regla in reglas:
        nombre    = regla["nombre"]
        condicion = regla["condicion"]
        critica   = regla.get("critica", False)

        try:
            n_fallos  = df.filter(~condicion).count()
            pct_fallo = round(n_fallos / n_total * 100, 2) if n_total > 0 else 0
            aprobada  = n_fallos == 0

            estado = "✅ APROBADA" if aprobada else ("❌ FALLIDA" if critica else "⚠️ ADVERTENCIA")

            print(f"  {nombre:<40} {estado:>10}  {pct_fallo:>10.1f}%")

            resultados[nombre] = {
                "aprobada":   aprobada,
                "n_fallos":   n_fallos,
                "pct_fallo":  pct_fallo,
                "critica":    critica
            }

        except Exception as e:
            print(f"  {nombre:<40} {'ERROR':>10}  {str(e)[:30]}")
            resultados[nombre] = {"aprobada": False, "error": str(e)}

    # Persistir resultados
    rows = [(tabla, n, r["aprobada"], r.get("n_fallos", 0),
             r.get("pct_fallo", 0), r.get("critica", False),
             datetime.now())
            for n, r in resultados.items()]

    df_res = spark.createDataFrame(
        rows,
        ["tabla", "prueba", "aprobada", "n_fallos",
         "pct_fallo", "critica", "ts_ejecucion"]
    ).withColumn("ts_ejecucion", F.col("ts_ejecucion").cast("timestamp"))

    df_res.write.format("delta").mode("append") \
          .save(f"{SILVER_BASE}/_control/pruebas_calidad")

    return resultados


# COMMAND ----------

# Función principal Silver

def procesar_silver(
    tabla: str,
    pk_cols: list,
    partition_col: str,
    estrategias_nulos: dict,
    reglas_calidad: list,
    transformaciones_fn=None
) -> dict:
    """
    Orquesta el procesamiento completo de Silver para una tabla.
    """
    lote_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    inicio  = datetime.now()

    # Crear cdf_versions si no existe
    try:
        spark.read.format("delta").load(CDF_VERSIONS_PATH)
    except:
        spark.createDataFrame(
            [(tabla, "silver", -1, datetime.now())],
            ["tabla", "capa", "ultima_version", "ts_actualizacion"]
        ).withColumn("ts_actualizacion", F.col("ts_actualizacion").cast("timestamp")) \
        .write.format("delta").mode("append").save(CDF_VERSIONS_PATH)
        print(f"  ✅ cdf_versions inicializada")

    print("=" * 60)
    print(f"  Silver: {tabla}")
    print(f"  Lote:   {lote_id}")
    print("=" * 60)

    try:
        # 1. Leer desde Bronze
        df_bronze, version_actual = leer_bronze(tabla)

        if df_bronze is None:
            print(f"  ℹ️  Sin cambios en Bronze para {tabla}")
            return {"tabla": tabla, "registros": 0, "estado": "SIN_CAMBIOS"}

        df = df_bronze

        # 2. Eliminar columnas de auditoría Bronze
        audit_cols = ["_ingesta_ts","_fuente","_lote_id","_anio","_mes","_dia"]
        df = df.drop(*[c for c in audit_cols if c in df.columns])

        # 3. Deduplicar
        df, n_dup = deduplicar(df, pk_cols, tabla)

        # 4. Manejar nulos
        df = manejar_nulos(df, estrategias_nulos)

        # 5. Enmascarar PII
        df = enmascarar_pii(df, tabla)

        # 6. Transformaciones específicas de la tabla
        n_rechazados = 0
        if transformaciones_fn:
            df, n_rechazados = transformaciones_fn(df, tabla)

        # 7. Pruebas de calidad
        ejecutar_pruebas_calidad(df, tabla, reglas_calidad)

        # 8. Reporte de calidad
        generar_reporte_calidad(df_bronze, df, tabla, lote_id, n_dup, n_rechazados)

        # 9. Escribir Silver (MERGE idempotente)
        n = escribir_silver(df, tabla, pk_cols, partition_col, lote_id)

        duracion = (datetime.now() - inicio).seconds
        print(f"\n✅ Silver {tabla} completado | {n:,} registros | {duracion}s")

        update_version_cdf(tabla, "bronze", version_actual)
        print(f"  📌 Versión CDF actualizada: bronze/{tabla} → v{version_actual}")

        return {"tabla": tabla, "registros": n, "estado": "EXITOSO"}

    except Exception as e:
        duracion = (datetime.now() - inicio).seconds
        error_msg = str(e)[:500]
        print(f"\n❌ Error Silver {tabla}: {error_msg}")

        # Log error sin interrumpir pipeline
        df_err = spark.createDataFrame(
            [(tabla, lote_id, datetime.now(), error_msg)],
            ["tabla", "lote_id", "ts_error", "mensaje"]
        ).withColumn("ts_error", F.col("ts_error").cast("timestamp"))
        df_err.write.format("delta").mode("append").save(ERRORES_PATH)

        return {"tabla": tabla, "registros": 0, "estado": "ERROR", "error": error_msg}

print("✅ silver_utils cargado correctamente")