# Databricks notebook source
# ── CELDA 1: Configuración ────────────────────────────────────
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
STORAGE_ACCOUNT = "dlshealthnetdev"
SECRET_SCOPE    = "healthnet-kv-scope"
 
access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-access-key")
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    access_key
)
 
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"
GOLD_BASE   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet"
 
UMBRAL_DESVIACION = 0.30  # 30% — parametrizable
N_EJECUCIONES     = 7     # últimas 7 ejecuciones para el promedio
 
print(f"✅ Configuración cargada")
print(f"   Umbral anomalía: {UMBRAL_DESVIACION*100:.0f}%")
print(f"   Ventana histórica: {N_EJECUCIONES} ejecuciones")

# COMMAND ----------

# ── CELDA 2: Leer logs de ejecución ──────────────────────────
def leer_log_bronze() -> object:
    """Lee el log de ejecuciones de Bronze."""
    try:
        return spark.read.format("delta").load(f"{BRONZE_BASE}/_control/pipeline_log")
    except Exception as e:
        print(f"⚠️  No se pudo leer log Bronze: {str(e)[:80]}")
        return None
 
def leer_log_silver() -> object:
    """Lee el log de ejecuciones de Silver."""
    try:
        return spark.read.format("delta").load(f"{SILVER_BASE}/_control/reporte_calidad")
    except Exception as e:
        print(f"⚠️  No se pudo leer log Silver: {str(e)[:80]}")
        return None
 
def leer_log_gold() -> object:
    """Lee el log de ejecuciones de Gold."""
    try:
        return spark.read.format("delta").load(f"{GOLD_BASE}/_control/pipeline_log")
    except Exception as e:
        print(f"⚠️  No se pudo leer log Gold: {str(e)[:80]}")
        return None

# COMMAND ----------

# ── CELDA 3: Detectar anomalías de volumen ───────────────────
def detectar_anomalias(df_log, capa: str) -> list:
    if df_log is None:
        return []

    anomalias = []

    try:
        df_ordenado = df_log.orderBy(F.col("ts_ejecucion").desc())
        tablas = [r["tabla"] for r in df_ordenado.select("tabla").distinct().collect()]

        for tabla in tablas:
            df_tabla = df_ordenado.filter(F.col("tabla") == tabla)

            # Última ejecución
            ultima = df_tabla.limit(1).collect()
            if not ultima:
                continue
            n_ultima = ultima[0]["registros_procesados"] if "registros_procesados" \
                       in df_tabla.columns else ultima[0].get("registros", 0)

            # Historial — excluir la ejecución actual
            historico = df_tabla.limit(N_EJECUCIONES + 1).tail(N_EJECUCIONES)

            # ── FIX: mínimo 3 ejecuciones para comparar ──────────
            if len(historico) < 3:
                print(f"  ℹ️  {capa}.{tabla}: historial insuficiente "
                      f"({len(historico)} ejecuciones) — omitiendo validación")
                continue
            # ─────────────────────────────────────────────────────

            n_historico = [
                r["registros_procesados"] if "registros_procesados" in df_tabla.columns
                else r.get("registros", 0)
                for r in historico
            ]
            promedio = sum(n_historico) / len(n_historico)

            if promedio == 0:
                continue

            desviacion = abs(n_ultima - promedio) / promedio

            if desviacion > UMBRAL_DESVIACION:
                anomalia = {
                    "capa":           capa,
                    "tabla":          tabla,
                    "n_actual":       n_ultima,
                    "n_promedio":     round(promedio, 0),
                    "pct_desviacion": round(desviacion * 100, 2),
                    "ts_deteccion":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                anomalias.append(anomalia)
                print(f"  🚨 ANOMALÍA: {capa}.{tabla} | "
                      f"Actual: {n_ultima:,} | "
                      f"Promedio: {promedio:,.0f} | "
                      f"Desviación: {desviacion*100:.1f}%")

    except Exception as e:
        print(f"  ⚠️  Error detectando anomalías en {capa}: {str(e)[:100]}")

    return anomalias
 

# COMMAND ----------

# ── CELDA 4: Registrar anomalías en tabla de control ─────────
ANOMALIAS_PATH = f"{GOLD_BASE}/_control/alertas_volumen"
 
def registrar_anomalias(anomalias: list):
    """Persiste las anomalías detectadas en tabla Delta."""
    if not anomalias:
        return
 
    df_alertas = spark.createDataFrame(anomalias)
    df_alertas.write.format("delta").mode("append").save(ANOMALIAS_PATH)
    print(f"  📋 {len(anomalias)} anomalías registradas en {ANOMALIAS_PATH}")

# COMMAND ----------

# ── CELDA 5: Enviar alerta via Log Analytics ──────────────────
def enviar_alerta_log_analytics(anomalias: list):
    """
    Envía las anomalías a Log Analytics via API REST.
    Azure Monitor puede disparar alertas sobre estos logs.
    """
    if not anomalias:
        return
 
    import requests
    import json
    import hashlib
    import hmac
    import base64
    import time
 
    try:
        workspace_id  = dbutils.secrets.get(scope=SECRET_SCOPE, key="log-analytics-workspace-id")
        workspace_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="log-analytics-workspace-key")
    except Exception:
        print("  ⚠️  Secrets de Log Analytics no configurados — omitiendo envío")
        return
 
    log_type = "HealthNetVolumeAnomaly"
    body     = json.dumps(anomalias)
 
    # Construir firma HMAC-SHA256
    rfc1123date = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
    content_length = len(body)
    string_to_hash = f"POST\n{content_length}\napplication/json\nx-ms-date:{rfc1123date}\n/api/logs"
    decoded_key    = base64.b64decode(workspace_key)
    signature      = base64.b64encode(
        hmac.new(decoded_key, string_to_hash.encode("utf-8"), digestmod=hashlib.sha256).digest()
    ).decode("utf-8")
 
    headers = {
        "Content-Type":  "application/json",
        "Log-Type":      log_type,
        "x-ms-date":     rfc1123date,
        "Authorization": f"SharedKey {workspace_id}:{signature}",
    }
 
    response = requests.post(
        f"https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01",
        data=body,
        headers=headers
    )
 
    if response.status_code == 200:
        print(f"  ✅ Alerta enviada a Log Analytics: {len(anomalias)} anomalías")
    else:
        print(f"  ⚠️  Error enviando a Log Analytics: {response.status_code}")

# COMMAND ----------

# ── CELDA 6: Orquestación principal ──────────────────────────
 
print("=" * 60)
print("  HealthNet — Validación de Anomalías de Volumen")
print(f"  Umbral: {UMBRAL_DESVIACION*100:.0f}% | Ventana: {N_EJECUCIONES} ejecuciones")
print("=" * 60)
 
# Leer logs de las 3 capas
df_bronze = leer_log_bronze()
df_silver = leer_log_silver()
df_gold   = leer_log_gold()
 
# Detectar anomalías por capa
print("\n── Analizando Bronze ────────────────────────────────────")
anomalias_bronze = detectar_anomalias(df_bronze, "Bronze")
 
print("\n── Analizando Silver ────────────────────────────────────")
anomalias_silver = detectar_anomalias(df_silver, "Silver")
 
print("\n── Analizando Gold ──────────────────────────────────────")
anomalias_gold = detectar_anomalias(df_gold, "Gold")
 
# Consolidar todas las anomalías
todas_anomalias = anomalias_bronze + anomalias_silver + anomalias_gold

# COMMAND ----------

# ── CELDA 7: Resultado y notificación ────────────────────────

print("\n" + "=" * 60)
print("  RESULTADO — VALIDACIÓN DE VOLUMEN")
print("=" * 60)
 
if todas_anomalias:
    print(f"\n  🚨 {len(todas_anomalias)} ANOMALÍAS DETECTADAS\n")
    print(f"  {'CAPA':<10} {'TABLA':<25} {'ACTUAL':>10} {'PROMEDIO':>10} {'DESV%':>8}")
    print(f"  {'-'*68}")
    for a in todas_anomalias:
        print(f"  {a['capa']:<10} {a['tabla']:<25} "
              f"{a['n_actual']:>10,} {a['n_promedio']:>10,.0f} "
              f"{a['pct_desviacion']:>7.1f}%")
 
    # Persistir anomalías
    registrar_anomalias(todas_anomalias)
 
    # Enviar a Log Analytics
    enviar_alerta_log_analytics(todas_anomalias)
 
    # Lanzar excepción para que ADF registre el fallo y dispare alerta email
    raise Exception(
        f"ANOMALIA_VOLUMEN: {len(todas_anomalias)} tablas con desviación > "
        f"{UMBRAL_DESVIACION*100:.0f}% respecto al promedio histórico. "
        f"Tablas afectadas: {[a['tabla'] for a in todas_anomalias]}"
    )
 
else:
    print(f"\n  ✅ Sin anomalías de volumen detectadas")
    print(f"  Todas las tablas dentro del umbral del {UMBRAL_DESVIACION*100:.0f}%")
    registrar_anomalias([{
        "capa": "ALL", "tabla": "ALL",
        "n_actual": 0, "n_promedio": 0,
        "pct_desviacion": 0.0,
        "ts_deteccion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "estado": "OK"
    }])