# Databricks notebook source
# Notebook: 00_subir_config_yaml
# Ejecutar UNA sola vez antes de correr fase_generacion_datos

STORAGE_ACCOUNT = "dlshealthnetdev"
SECRET_SCOPE    = "healthnet-kv-scope"

access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-access-key")
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    access_key
)

config_yaml = """
seed: 17

date_range:
  start: "2024-01-01"
  end:   "2024-12-31"

volumes:
  RED_SEDES:          82
  MED_PLANTA:       2000
  PAC_REGISTRO:   100000
  AGE_CITAS:      1500000
  HCE_ENCUENTROS: 2000000
  GCM_CAMAS:       500000
  FAR_DISPENSACION: 3000000

null_rate: 0.05

paises:
  Colombia: 45
  Peru:     22
  Ecuador:  15

ciudades:
  Colombia:
    - { ciudad: "Bogota",       departamento: "Cundinamarca",    id_ciudad: 1 }
    - { ciudad: "Medellin",     departamento: "Antioquia",       id_ciudad: 2 }
    - { ciudad: "Cali",         departamento: "Valle del Cauca", id_ciudad: 3 }
    - { ciudad: "Barranquilla", departamento: "Atlantico",       id_ciudad: 4 }
    - { ciudad: "Bucaramanga",  departamento: "Santander",       id_ciudad: 5 }
    - { ciudad: "Cartagena",    departamento: "Bolivar",         id_ciudad: 6 }
    - { ciudad: "Pereira",      departamento: "Risaralda",       id_ciudad: 7 }
    - { ciudad: "Manizales",    departamento: "Caldas",          id_ciudad: 8 }
  Peru:
    - { ciudad: "Lima",         departamento: "Lima",            id_ciudad: 20 }
    - { ciudad: "Arequipa",     departamento: "Arequipa",        id_ciudad: 21 }
    - { ciudad: "Trujillo",     departamento: "La Libertad",     id_ciudad: 22 }
    - { ciudad: "Chiclayo",     departamento: "Lambayeque",      id_ciudad: 23 }
  Ecuador:
    - { ciudad: "Quito",        departamento: "Pichincha",       id_ciudad: 30 }
    - { ciudad: "Guayaquil",    departamento: "Guayas",          id_ciudad: 31 }
    - { ciudad: "Cuenca",       departamento: "Azuay",           id_ciudad: 32 }

tipos_sede:
  - { tipo: "Hospital Alta Complejidad",   nivel: 3, cantidad: 3,
      camas_gen_min: 100, camas_gen_max: 250,
      camas_uci_min: 20,  camas_uci_max: 60,
      camas_cir_min: 15,  camas_cir_max: 40,
      camas_urg_min: 30,  camas_urg_max: 80 }
  - { tipo: "Clinica Mediana Complejidad", nivel: 2, cantidad: 16,
      camas_gen_min: 30,  camas_gen_max: 80,
      camas_uci_min: 5,   camas_uci_max: 15,
      camas_cir_min: 5,   camas_cir_max: 12,
      camas_urg_min: 8,   camas_urg_max: 20 }
  - { tipo: "Centro Medico Ambulatorio",   nivel: 1, cantidad: 42,
      camas_gen_min: 2,   camas_gen_max: 10,
      camas_uci_min: 0,   camas_uci_max: 0,
      camas_cir_min: 0,   camas_cir_max: 2,
      camas_urg_min: 2,   camas_urg_max: 6 }
  - { tipo: "Centro Diagnostico",          nivel: 1, cantidad: 21,
      camas_gen_min: 0,   camas_gen_max: 2,
      camas_uci_min: 0,   camas_uci_max: 0,
      camas_cir_min: 0,   camas_cir_max: 0,
      camas_urg_min: 0,   camas_urg_max: 0 }

umbrales_ocupacion:
  UCI:       { critico: 0.85, precaucion: 0.70 }
  Urgencias: { critico: 0.90, precaucion: 0.75 }
  General:   { critico: 0.88, precaucion: 0.72 }
  Cirugia:   { critico: 0.88, precaucion: 0.72 }

umbral_brote_epidemiologico: 0.40

especialidades:
  - Medicina General
  - Urgencias
  - Pediatria
  - Ginecologia
  - Cardiologia
  - Neurologia
  - Ortopedia
  - Oncologia
  - Dermatologia
  - Gastroenterologia
  - Oftalmologia
  - Psiquiatria
  - Endocrinologia
  - Neumologia

aseguradoras:
  - { nombre: "EPS Sanitas",         tipo: "EPS",        pais: "Colombia", id: "EPS001" }
  - { nombre: "EPS Sura",            tipo: "EPS",        pais: "Colombia", id: "EPS002" }
  - { nombre: "Nueva EPS",           tipo: "EPS",        pais: "Colombia", id: "EPS003" }
  - { nombre: "Compensar",           tipo: "EPS",        pais: "Colombia", id: "EPS004" }
  - { nombre: "Famisanar",           tipo: "EPS",        pais: "Colombia", id: "EPS005" }
  - { nombre: "Salud Total",         tipo: "EPS",        pais: "Colombia", id: "EPS006" }
  - { nombre: "Coosalud",            tipo: "EPS",        pais: "Colombia", id: "EPS007" }
  - { nombre: "Medimas",             tipo: "EPS",        pais: "Colombia", id: "EPS008" }
  - { nombre: "Particular",          tipo: "Particular", pais: "Todos",    id: "PAR000" }
  - { nombre: "Rimac Seguros",       tipo: "Seguro",     pais: "Peru",     id: "SEG001" }
  - { nombre: "Pacifico Seguros",    tipo: "Seguro",     pais: "Peru",     id: "SEG002" }
  - { nombre: "La Positiva Seguros", tipo: "Seguro",     pais: "Peru",     id: "SEG003" }
  - { nombre: "IESS Ecuador",        tipo: "Seguro",     pais: "Ecuador",  id: "SEG004" }
  - { nombre: "Seguros Sucre",       tipo: "Seguro",     pais: "Ecuador",  id: "SEG005" }

cie10:
  - { codigo: "J06", descripcion: "Infec. aguda vias resp. superiores",  cronico: false, oncologico: false }
  - { codigo: "J18", descripcion: "Neumonia",                            cronico: false, oncologico: false }
  - { codigo: "K29", descripcion: "Gastritis y duodenitis",              cronico: false, oncologico: false }
  - { codigo: "I10", descripcion: "Hipertension esencial",               cronico: true,  oncologico: false }
  - { codigo: "E11", descripcion: "Diabetes mellitus tipo 2",            cronico: true,  oncologico: false }
  - { codigo: "J45", descripcion: "Asma",                                cronico: true,  oncologico: false }
  - { codigo: "A09", descripcion: "Diarrea y gastroenteritis",           cronico: false, oncologico: false }
  - { codigo: "N39", descripcion: "Infeccion vias urinarias",            cronico: false, oncologico: false }
  - { codigo: "M54", descripcion: "Dorsalgia",                           cronico: false, oncologico: false }
  - { codigo: "F32", descripcion: "Episodio depresivo",                  cronico: true,  oncologico: false }
  - { codigo: "C50", descripcion: "Tumor maligno de mama",               cronico: false, oncologico: true  }
  - { codigo: "C34", descripcion: "Tumor maligno de bronquio y pulmon",  cronico: false, oncologico: true  }
  - { codigo: "Z00", descripcion: "Examen general sin queja",            cronico: false, oncologico: false }
  - { codigo: "I25", descripcion: "Enfermedad isquemica cronica corazon",cronico: true,  oncologico: false }
  - { codigo: "E14", descripcion: "Diabetes mellitus no especificada",   cronico: true,  oncologico: false }

anomalias:
  duplicados_age_citas:
    descripcion: "~0.5% de registros duplicados en AGE_CITAS (mismo pac_id, id_sede, fec_cita_programada)"
    tasa: 0.005
  fechas_fuera_rango_hce:
    descripcion: "~0.3% de HCE_ENCUENTROS con fec_registro anterior a 2023-01-01"
    tasa: 0.003
  camas_inconsistentes:
    descripcion: "~0.4% de GCM_CAMAS con num_camas_disp negativo"
    tasa: 0.004

output_formats:
  - parquet
  - csv

json_tables:
  - FAR_DISPENSACION

adls:
  storage_account: "dlshealthnetdev"
  container_bronze: "bronze"
  container_silver: "silver"
  container_gold:   "gold"

azure_sql:
  server:          "sql-healthnet-dev.database.windows.net"
  database:        "healthnet-source"
  port:            1433
  driver:          "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  secret_scope:    "healthnet-kv-scope"
  secret_key_user: "sql-user"
  secret_key_pass: "sql-password"
"""

# Subir a ADLS Gen2
CONFIG_ADLS = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/healthnet/config/generation_config.yaml"
dbutils.fs.put(CONFIG_ADLS, config_yaml, overwrite=True)
print(f"✅ Config subida a: {CONFIG_ADLS}")