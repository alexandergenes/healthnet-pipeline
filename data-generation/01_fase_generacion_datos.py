# Databricks notebook source
# Configuración de acceso a ADLS Gen2

STORAGE_ACCOUNT = "dlshealthnetdev"
SECRET_SCOPE    = "healthnet-kv-scope"
SECRET_KEY      = "storage-access-key"

access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    access_key
)

LANDING_PATH = f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet/raw"
CONFIG_PATH  = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet/config/generation_config.yaml"

print(f"✅ Acceso ADLS Gen2 configurado")
print(f"   Landing: {LANDING_PATH}")

# COMMAND ----------

# Leer offsets desde Azure SQL via Spark

server   = dbutils.secrets.get(scope="healthnet-kv-scope", key="sql-server")
user     = dbutils.secrets.get(scope="healthnet-kv-scope", key="sql-user")
password = dbutils.secrets.get(scope="healthnet-kv-scope", key="sql-password")
database = "healthnet-source"

JDBC_URL = f"jdbc:sqlserver://{server};databaseName={database};encrypt=true;trustServerCertificate=false"
JDBC_PROPS = {
    "user":     user,
    "password": password,
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

query = """(
    SELECT 'RED_SEDES' AS tabla, ISNULL(MAX(id_sede), 0) AS max_id FROM dbo.RED_SEDES
    UNION ALL
    SELECT 'MED_PLANTA', ISNULL(MAX(med_id), 0) FROM dbo.MED_PLANTA
    UNION ALL
    SELECT 'PAC_REGISTRO', ISNULL(MAX(pac_id), 0) FROM dbo.PAC_REGISTRO
    UNION ALL
    SELECT 'AGE_CITAS', ISNULL(MAX(id_cita), 0) FROM dbo.AGE_CITAS
    UNION ALL
    SELECT 'HCE_ENCUENTROS', ISNULL(MAX(id_encuentro), 0) FROM dbo.HCE_ENCUENTROS
    UNION ALL
    SELECT 'GCM_CAMAS', ISNULL(MAX(id_registro_cama), 0) FROM dbo.GCM_CAMAS
    UNION ALL
    SELECT 'FAR_DISPENSACION', ISNULL(MAX(id_dispensacion), 0) FROM dbo.FAR_DISPENSACION
) t"""

df_offsets = spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)
OFFSETS    = {row["tabla"]: row["max_id"] for row in df_offsets.collect()}

print("✅ Offsets leídos desde Azure SQL:")
for tabla, offset in OFFSETS.items():
    print(f"  {tabla}: {offset:,}")

# COMMAND ----------

# Cargar configuración desde ADLS
import yaml, hashlib, random
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from faker import Faker

# Leer YAML desde ADLS Gen2
config_content = dbutils.fs.head(CONFIG_PATH, 100000)
cfg = yaml.safe_load(config_content)

SEED       = cfg["seed"]
DT_START   = datetime.strptime(cfg["date_range"]["start"], "%Y-%m-%d").date()
DT_END     = datetime.strptime(cfg["date_range"]["end"],   "%Y-%m-%d").date()
NULL_RATE  = cfg["null_rate"]
VOL        = cfg["volumes"]

random.seed(SEED)
np.random.seed(SEED)
fake = Faker("es_CO")
fake.seed_instance(SEED)

CIUDADES_BY_PAIS = {pais: data for pais, data in cfg["ciudades"].items()}
ASEGURADORAS     = cfg["aseguradoras"]
CIE10            = cfg["cie10"]
ESPECIALIDADES   = cfg["especialidades"]

print(f"✅ Config cargada | Seed={SEED} | {DT_START} → {DT_END}")
for t, v in VOL.items():
    print(f"   {t:<22} {v:>10,} registros")



# COMMAND ----------

# Funciones helper

def rdate(start=DT_START, end=DT_END) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))

def rdatetime(start=DT_START, end=DT_END) -> datetime:
    d      = rdate(start, end)
    hora   = random.choices(
                range(24),
                weights=[0,0,0,0,0,0,1,3,5,5,5,5,3,2,5,5,5,4,2,1,1,1,1,1]
             )[0]
    minuto = random.choice([0, 15, 30, 45])
    return datetime(d.year, d.month, d.day, hora, minuto)

def maybe_null(val, rate=NULL_RATE):
    return None if random.random() < rate else val

def sha256_hash(valor: str) -> str:
    return hashlib.sha256(valor.encode()).hexdigest()

def to_spark(df_pd: pd.DataFrame, name: str):
    sdf = spark.createDataFrame(df_pd)
    print(f"  ✔ {name:<22} {sdf.count():>10,} filas | {len(df_pd.columns)} cols")
    return sdf



# COMMAND ----------

# RED_SEDES

def gen_red_sedes() -> pd.DataFrame:
    rows = []
    sede_id = 1
    tipo_pool = []
    for t in cfg["tipos_sede"]:
        tipo_pool.extend([t] * t["cantidad"])
    random.shuffle(tipo_pool)

    pais_pool = []
    for pais, n in cfg["paises"].items():
        pais_pool.extend([pais] * n)
    random.shuffle(pais_pool)

    pais_ids = {"Colombia": 1, "Peru": 2, "Ecuador": 3}

    for i, pais in enumerate(pais_pool):
        tipo_cfg   = tipo_pool[i] if i < len(tipo_pool) else tipo_pool[-1]
        ciudad_obj = random.choice(CIUDADES_BY_PAIS[pais])
        rows.append({
            "id_sede":           sede_id,
            "nom_sede":          f"HealthNet {ciudad_obj['ciudad']} {i+1}",
            "tip_sede":          tipo_cfg["tipo"],
            "id_ciudad":         ciudad_obj["id_ciudad"],
            "nom_ciudad":        ciudad_obj["ciudad"],
            "id_pais":           pais_ids[pais],
            "nom_pais":          pais,
            "nivel_complejidad": tipo_cfg["nivel"],
            "cap_camas_gen":     random.randint(tipo_cfg["camas_gen_min"], max(tipo_cfg["camas_gen_min"], tipo_cfg["camas_gen_max"])),
            "cap_camas_uci":     random.randint(tipo_cfg["camas_uci_min"], max(tipo_cfg["camas_uci_min"], tipo_cfg["camas_uci_max"])),
            "cap_camas_cirugia": random.randint(tipo_cfg["camas_cir_min"], max(tipo_cfg["camas_cir_min"], tipo_cfg["camas_cir_max"])),
            "cap_camas_urg":     random.randint(tipo_cfg["camas_urg_min"], max(tipo_cfg["camas_urg_min"], tipo_cfg["camas_urg_max"])),
            "activa":            True,
            "fec_apertura":      maybe_null(str(rdate(date(1990,1,1), date(2020,12,31)))),
        })
        sede_id += 1
    return pd.DataFrame(rows)

# COMMAND ----------

#  MED_PLANTA

def gen_med_planta(sede_ids: list) -> pd.DataFrame:
    rows      = []
    contratos = ["Planta", "Planta", "Prestacion Servicios", "Honorarios"]
    jornadas  = ["Completa", "Completa", "Medio Tiempo", "Turno"]
    for med_id in range(1, VOL["MED_PLANTA"] + 1):
        esp = random.choice(ESPECIALIDADES)
        rows.append({
            "med_id":         med_id,
            "esp_principal":  esp,
            "esp_secundaria": maybe_null(random.choice([e for e in ESPECIALIDADES if e != esp])),
            "id_sede":        random.choice(sede_ids),
            "fec_ingreso":    str(rdate(date(2005,1,1), date(2023,12,31))),
            "tip_contrato":   random.choice(contratos),
            "jornada":        random.choice(jornadas),
            "estado_activo":  random.random() > 0.04,
        })
    return pd.DataFrame(rows)

# COMMAND ----------

#  PAC_REGISTRO 

def gen_pac_registro() -> pd.DataFrame:
    rows  = []
    edades = np.clip(np.random.normal(40, 18, VOL["PAC_REGISTRO"]).astype(int), 0, 95)
    
    for pac_id in range(1, VOL["PAC_REGISTRO"] + 1):
        edad    = int(edades[pac_id - 1])
        fec_nac = date.today() - relativedelta(years=edad, months=random.randint(0,11), days=random.randint(0,28))

        if edad < 7:    tip = "RC"
        elif edad < 18: tip = "TI"
        else:           tip = random.choices(["CC","CC","CC","CE","PAS"], weights=[6,6,6,1,1])[0]

        num_doc_hash = sha256_hash(str(random.randint(10000000, 1999999999)))

        pais_res = random.choices(["Colombia","Peru","Ecuador","Otro"], weights=[60,22,15,3])[0]
        if pais_res in CIUDADES_BY_PAIS:
            co        = random.choice(CIUDADES_BY_PAIS[pais_res])
            id_c, nom_c = co["id_ciudad"], co["ciudad"]
        else:
            id_c, nom_c = None, "Exterior"

        aseg    = maybe_null(random.choice(ASEGURADORAS))
        estrato = random.choices([1,2,3,4,5,6], weights=[15,30,28,15,8,4])[0] if pais_res == "Colombia" else None

        rows.append({
            "pac_id":             pac_id,
            "tip_doc":            tip,
            "num_doc_hash":       num_doc_hash,
            "fec_nac":            str(fec_nac),
            "genero":             random.choices(["F","M","O"], weights=[52,47,1])[0],
            "id_ciudad_res":      maybe_null(id_c),
            "nom_ciudad_res":     maybe_null(nom_c),
            "tip_aseguradora":    aseg["nombre"] if aseg else None,
            "id_eps":             aseg["id"]     if aseg else None,
            "estrato_socioec":    maybe_null(estrato),
            "fec_primer_atencion":maybe_null(str(rdate(date(2020,1,1), DT_END))),
            "activo":             random.random() > 0.02,
        })
        if pac_id % 20000 == 0:
            print(f"    PAC_REGISTRO: {pac_id:,} / {VOL['PAC_REGISTRO']:,}...")
    return pd.DataFrame(rows)

# COMMAND ----------

# AGE_CITAS
# ANOMALÍA 1: ~0.5% duplicados (mismo pac, sede, fecha)

def gen_age_citas(pac_ids, med_ids, sede_ids) -> pd.DataFrame:
    rows     = []
    estados  = ["Atendida","Atendida","Atendida","Cancelada","No Asistio","Programada"]
    tip_cita = ["Primera Vez","Control","Control","Control"]

    for cita_id in range(1, VOL["AGE_CITAS"] + 1):
        fec_prog  = rdate()
        hora_pico = random.choices(
            [random.randint(8,11), random.randint(14,16), random.randint(7,18)],
            weights=[4,3,1]
        )[0]
        minuto = random.choice([0,20,40])
        estado = random.choice(estados)

        if estado == "Atendida":
            fec_dt   = datetime(fec_prog.year, fec_prog.month, fec_prog.day, hora_pico, minuto)
            espera_m = int(np.clip(np.random.lognormal(3.4, 0.7), 5, 180))
            llegada  = fec_dt - timedelta(minutes=random.randint(0,20))
            inicio   = llegada + timedelta(minutes=espera_m)
            hra_llegada = maybe_null(str(llegada))
            hra_inicio  = maybe_null(str(inicio))
        else:
            hra_llegada = None
            hra_inicio  = None

        pac = random.choice(pac_ids)
        sed = random.choice(sede_ids)

        # ANOMALÍA 1: duplicados ~0.5%
        if random.random() < cfg["anomalias"]["duplicados_age_citas"]["tasa"] and rows:
            orig     = random.choice(rows[-200:] if len(rows) >= 200 else rows)
            pac      = orig["pac_id"]
            sed      = orig["id_sede"]
            fec_prog = datetime.strptime(orig["fec_cita_programada"], "%Y-%m-%d").date()

        rows.append({
            "id_cita":             cita_id,
            "pac_id":              pac,
            "med_id":              random.choice(med_ids),
            "id_sede":             sed,
            "fec_agendamiento":    str(fec_prog - timedelta(days=random.randint(1,30))),
            "fec_cita_programada": str(fec_prog),
            "hra_cita_programada": f"{hora_pico:02d}:{minuto:02d}:00",
            "hra_llegada_paciente":hra_llegada,
            "hra_inicio_atencion": hra_inicio,
            "esp_solicitada":      random.choice(ESPECIALIDADES),
            "tip_cita":            random.choice(tip_cita),
            "estado_cita":         estado,
        })
        if cita_id % 200000 == 0:
            print(f"    AGE_CITAS: {cita_id:,} / {VOL['AGE_CITAS']:,}...")
    return pd.DataFrame(rows)

# COMMAND ----------

# HCE_ENCUENTROS

def gen_hce_encuentros(pac_ids, med_ids, sede_ids) -> pd.DataFrame:
    rows   = []
    tipos_consulta  = ["Primera Vez", "Control", "Urgencia", 
                       "Hospitalizacion", "Cirugia"]
    pesos_consulta  = [20, 35, 15, 8, 5]
    estados_factura = ["Radicada", "Pagada", "Glosada", "Devuelta"]
    pesos_factura   = [40, 35, 15, 10]

    for enc_id in range(1, VOL["HCE_ENCUENTROS"] + 1):
        # ANOMALÍA 2: ~0.3% fechas fuera de rango
        if random.random() < cfg["anomalias"]["fechas_fuera_rango_hce"]["tasa"]:
            fec_reg = rdatetime(date(2020,1,1), date(2022,12,31))
        else:
            fec_reg = rdatetime()

        tip        = random.choices(tipos_consulta, weights=pesos_consulta)[0]
        fec_inicio = fec_reg + timedelta(minutes=random.randint(5,120))

        if tip in ("Hospitalizacion", "Cirugia"):
            fec_egreso = str(fec_inicio + timedelta(days=random.randint(1,25)))
        elif tip == "Urgencia":
            fec_egreso = maybe_null(str(fec_inicio + timedelta(hours=random.randint(1,48))))
        else:
            fec_egreso = None

        cie = random.choice(CIE10)
        cie_sec = maybe_null(
            random.choice([c for c in CIE10 if c["codigo"] != cie["codigo"]])["codigo"]
        )

        rows.append({
            "id_encuentro":         enc_id,
            "pac_id":               random.choice(pac_ids),
            "med_id":               random.choice(med_ids),
            "id_sede":              random.choice(sede_ids),
            "fec_registro":         str(fec_reg),
            "fec_inicio_atencion":  str(fec_inicio),
            "fec_egreso":           fec_egreso,
            "tip_consulta":         tip,
            "esp_atendida":         random.choice(ESPECIALIDADES),
            "diag_principal_cie10": cie["codigo"] + "." + str(random.randint(0,9)),
            "diag_sec1_cie10":      cie_sec,
            "cod_procedimientos":   maybe_null(",".join([f"P{random.randint(100,999)}" 
                                    for _ in range(random.randint(1,4))])),
            "vr_facturado":         maybe_null(round(random.uniform(30000, 5000000), 2)),
            "estado_factura":       maybe_null(random.choices(
                                        estados_factura, weights=pesos_factura)[0]),
        })
        if enc_id % 50000 == 0:
            print(f"    HCE_ENCUENTROS: {enc_id:,} / {VOL['HCE_ENCUENTROS']:,}...")
    return pd.DataFrame(rows)

# COMMAND ----------

# GCM_CAMAS
# ANOMALÍA 3: ~0.4% num_camas_disp negativo

def gen_gcm_camas(sedes_df: pd.DataFrame) -> pd.DataFrame:
    rows     = []
    reg_id   = 1
    cap_cols = {"General":"cap_camas_gen","UCI":"cap_camas_uci",
                "Cirugia":"cap_camas_cirugia","Urgencias":"cap_camas_urg"}
    motivos  = ["Mantenimiento preventivo","Limpieza profunda","Falla equipos",
                "Remodelacion","Sin personal disponible", None]
    n_por_sede = max(1, VOL["GCM_CAMAS"] // len(sedes_df))

    for _, sede in sedes_df.iterrows():
        for _ in range(n_por_sede):
            tip_u    = random.choice(list(cap_cols.keys()))
            cap      = max(2, int(sede[cap_cols[tip_u]]))
            tasa     = float(np.clip(np.random.beta(6,2), 0.2, 1.0))
            ocupadas = max(0, int(cap * tasa))
            disp     = cap - ocupadas
            # ANOMALÍA 3
            if random.random() < cfg["anomalias"]["camas_inconsistentes"]["tasa"]:
                disp = -random.randint(1,5)
            rows.append({
                "id_registro_cama":       reg_id,
                "id_sede":                int(sede["id_sede"]),
                "tip_unidad":             tip_u,
                "fec_hora_registro":      str(rdatetime()),
                "num_camas_ocupadas":     ocupadas,
                "num_camas_disp":         disp,
                "num_camas_mant":         maybe_null(random.randint(0, max(1, cap//10))),
                "motivo_indisponibilidad":maybe_null(random.choice(motivos)),
            })
            reg_id += 1
            if reg_id > VOL["GCM_CAMAS"]: break
        if reg_id > VOL["GCM_CAMAS"]: break
    return pd.DataFrame(rows)

# COMMAND ----------

# FAR_DISPENSACION

MEDICAMENTOS = [
    ("MET001","Metformina 500mg"),    ("AML002","Amlodipino 5mg"),
    ("OME003","Omeprazol 20mg"),      ("IBU004","Ibuprofeno 400mg"),
    ("AMO005","Amoxicilina 500mg"),   ("SAL006","Salbutamol 100mcg"),
    ("ATR007","Atorvastatina 40mg"),  ("LEV008","Levotiroxina 100mcg"),
    ("PAR009","Paracetamol 500mg"),   ("DIG010","Digoxina 0.25mg"),
    ("CIP011","Ciprofloxacina 500mg"),("INS012","Insulina NPH 100UI"),
    ("CLC013","Calcio + Vitamina D"), ("ASP014","Aspirina 100mg"),
    ("CLN015","Clonazepam 0.5mg"),
]

def gen_far_dispensacion(enc_ids, pac_ids, sede_ids) -> pd.DataFrame:
    rows      = []
    prescrips = ["Formulado","Formulado","Formulado","Venta Libre","Muestra Medica"]
    for disp_id in range(1, VOL["FAR_DISPENSACION"] + 1):
        med  = random.choice(MEDICAMENTOS)
        cant = random.randint(1, 90)
        v_u  = round(random.uniform(500, 80000), 2)
        rows.append({
            "id_dispensacion": disp_id,
            "id_encuentro":    maybe_null(random.choice(enc_ids)),
            "pac_id":          random.choice(pac_ids),
            "id_sede":         random.choice(sede_ids),
            "fec_dispensacion":str(rdatetime()),
            "cod_medicamento": med[0],
            "nom_medicamento": med[1],
            "cantidad":        cant,
            "vr_unitario":     maybe_null(v_u),
            "tip_prescripcion":maybe_null(random.choice(prescrips)),
        })
        if disp_id % 100000 == 0:
            print(f"    FAR_DISPENSACION: {disp_id:,} / {VOL['FAR_DISPENSACION']:,}...")
    return pd.DataFrame(rows)



# COMMAND ----------

# Orquestación

from datetime import datetime as dt
inicio_total = dt.now()

print("=" * 60)
print("  HealthNet — Generación de Datos Sintéticos — Fase 1")
print(f"  Seed {SEED} | {DT_START} → {DT_END}")
print("=" * 60)

print("\n[1/7] RED_SEDES ...")
df_sedes   = gen_red_sedes();          sede_ids = df_sedes["id_sede"].tolist()

print("\n[2/7] MED_PLANTA ...")
df_medicos = gen_med_planta(sede_ids); med_ids  = df_medicos["med_id"].tolist()

print("\n[3/7] PAC_REGISTRO ...")
df_pacs    = gen_pac_registro();       pac_ids  = df_pacs["pac_id"].tolist()

print("\n[4/7] AGE_CITAS ...")
df_citas   = gen_age_citas(pac_ids, med_ids, sede_ids)

print("\n[5/7] HCE_ENCUENTROS ...")
df_enc     = gen_hce_encuentros(pac_ids, med_ids, sede_ids)
enc_ids    = df_enc["id_encuentro"].tolist()

print("\n[6/7] GCM_CAMAS ...")
df_camas   = gen_gcm_camas(df_sedes)

print("\n[7/7] FAR_DISPENSACION ...")
df_far     = gen_far_dispensacion(enc_ids, pac_ids, sede_ids)

TABLAS = {
    "RED_SEDES":        df_sedes,
    "MED_PLANTA":       df_medicos,
    "PAC_REGISTRO":     df_pacs,
    "AGE_CITAS":        df_citas,
    "HCE_ENCUENTROS":   df_enc,
    "GCM_CAMAS":        df_camas,
    "FAR_DISPENSACION": df_far,
}

print("\n── Convirtiendo a Spark DataFrames ─────────────────────")
SPARK_DFS = {nombre: to_spark(df, nombre) for nombre, df in TABLAS.items()}



# COMMAND ----------

# Persistencia en landing (un formato por tabla) ──

ts_lote = dt.now().strftime("%Y%m%d_%H%M%S")

# Formato asignado por tabla — simula fuentes heterogéneas reales
FORMATO_TABLA = {
    "RED_SEDES":        "csv",
    "MED_PLANTA":       "csv",
    "PAC_REGISTRO":     "parquet",
    "AGE_CITAS":        "parquet",
    "HCE_ENCUENTROS":   "parquet",
    "GCM_CAMAS":        "json",
    "FAR_DISPENSACION": "json",
}

print("── Escribiendo en landing/ ──────────────────────────────")

for nombre, sdf in SPARK_DFS.items():
    formato = FORMATO_TABLA[nombre]
    path    = f"{LANDING_PATH}/{nombre}"

    if formato == "csv":
        sdf.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    elif formato == "parquet":
        sdf.coalesce(1).write.mode("overwrite").parquet(path)
    elif formato == "json":
        sdf.coalesce(1).write.mode("overwrite").json(path)

    print(f"  ✔ {nombre:<25} → {formato.upper():>7}  →  {path}")

print(f"\n✅ landing/ escrito correctamente. Lote: {ts_lote}")



# COMMAND ----------

# Validación — evidencia de carga en archivos

duracion = (dt.now() - inicio_total).seconds

print("\n" + "=" * 65)
print("  EVIDENCIA DE GENERACIÓN — COUNT(*) POR TABLA")
print("=" * 65)
print(f"{'TABLA':<25} {'FILAS':>12}  {'COLS':>5}  {'NULOS%':>8}")
print("-" * 57)

total_filas = 0
for nombre, sdf in SPARK_DFS.items():
    n = sdf.count()
    total_filas += n
    print(f"{nombre:<25} {n:>12,}")

print("-" * 57)
print(f"{'TOTAL':<25} {total_filas:>12,}")
print(f"\n⏱  Duración total: {duracion} segundos")

print("\n── Anomalías intencionales documentadas ─────────────────")
for k, v in cfg["anomalias"].items():
    print(f"  ⚠️  {k}")
    print(f"     {v['descripcion']}")

print(f"\n✅ Fase 1 completada. Lote: {ts_lote}")