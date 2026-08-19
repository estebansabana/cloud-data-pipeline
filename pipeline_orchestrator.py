"""
Deliverable 1 - Pipeline consolidado con arquitectura Medallion.
Herramientas de Computacion en la Nube - Universidad de La Sabana.

Ejecucion:  python pipeline_orchestrator.py
Requisitos: .env con FOURSQUARE_API_KEY, credenciales AWS y S3_BUCKET_NAME.
"""

import os
import sys
import ast
import io
import requests
import pandas as pd
import boto3
import duckdb
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

# --- Configuracion externalizada (Zero Trust: nada hardcodeado) ---
load_dotenv(".env", override=True)

FOURSQUARE_URL = "https://places-api.foursquare.com/places/search"
FSQ_API_VERSION = "2025-06-17"
BUCKET = os.getenv("S3_BUCKET_NAME")

OUTPUT_DIR = "output"
RAW_CSV = f"{OUTPUT_DIR}/restaurants_raw.csv"
OPTIMIZED_PARQUET = f"{OUTPUT_DIR}/restaurants_processed.parquet"
CONSUMPTION_PARQUET = f"{OUTPUT_DIR}/category_kpis.parquet"

KEY_RAW = "raw_zone/restaurants_raw.csv"
KEY_OPTIMIZED = "optimized_zone/restaurants_processed.parquet"
KEY_CONSUMPTION = "consumption_zone/category_kpis.parquet"

# Regla de negocio: umbral maximo de registros invalidos tolerado
QUALITY_THRESHOLD = 0.20


class DataQualityError(Exception):
    """El dataset no supera el umbral minimo de calidad definido."""


@task(name="Extraccion Foursquare", retries=2, retry_delay_seconds=5)
def extract_places() -> pd.DataFrame:
    """Consume la Places API y normaliza la respuesta JSON a DataFrame."""
    logger = get_run_logger()
    api_key = os.getenv("FOURSQUARE_API_KEY")
    if not api_key:
        raise ValueError("FOURSQUARE_API_KEY no encontrada en .env")

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key.strip()}",
        "X-Places-Api-Version": FSQ_API_VERSION,
    }
    params = {"query": "restaurants", "ll": "4.85876,-74.05866", "limit": 50}

    response = requests.get(FOURSQUARE_URL, headers=headers,
                            params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    df = pd.json_normalize(data["results"])
    df = df[["name", "location.address", "categories",
             "latitude", "longitude", "distance"]]
    logger.info(f"Extraccion completada: {len(df)} registros obtenidos.")
    return df


@task(name="Validacion de calidad")
def validate_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica reglas de calidad y verifica el umbral de negocio.
    Regla 1: descarta registros sin latitud o longitud.
    Regla 2: descarta registros con distance no numerica o negativa.
    """
    logger = get_run_logger()
    total = len(df)

    # Regla 1: coordenadas obligatorias (sin ellas no hay analisis geoespacial)
    sin_coords = df[["latitude", "longitude"]].isna().any(axis=1).sum()
    df_v = df.dropna(subset=["latitude", "longitude"])

    # Regla 2: distancia numerica y no negativa (una distancia < 0 es imposible)
    df_v = df_v.copy()
    df_v["distance"] = pd.to_numeric(df_v["distance"], errors="coerce")
    dist_invalida = df_v["distance"].isna().sum() + (df_v["distance"] < 0).sum()
    df_v = df_v[df_v["distance"].notna() & (df_v["distance"] >= 0)]

    validos = len(df_v)
    tasa_invalidez = (total - validos) / total if total else 1.0

    logger.info(f"Registros iniciales: {total}")
    logger.info(f"Descartados por coordenadas nulas: {sin_coords}")
    logger.info(f"Descartados por distancia invalida: {dist_invalida}")
    logger.info(f"Registros validos: {validos} "
                f"(tasa de invalidez: {tasa_invalidez:.2%})")

    if tasa_invalidez > QUALITY_THRESHOLD:
        raise DataQualityError(
            f"Tasa de invalidez {tasa_invalidez:.2%} supera el umbral "
            f"de {QUALITY_THRESHOLD:.0%}. Se aborta la carga a S3."
        )

    return df_v


def get_s3_client():
    """
    Construye el cliente S3 con credenciales temporales de sesion (STS).
    No es @task: es una factoria de recursos, no un paso del pipeline.
    """
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )


@task(name="Persistencia CSV local")
def save_raw_csv(df: pd.DataFrame) -> str:
    """Escribe el dataset crudo en disco local antes de tocar la nube."""
    logger = get_run_logger()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(RAW_CSV, index=False)
    tam = os.path.getsize(RAW_CSV)
    logger.info(f"CSV local generado: {RAW_CSV} ({tam} bytes)")
    return RAW_CSV


@task(name="Carga a S3", retries=1, retry_delay_seconds=3)
def upload_to_s3(local_path: str, s3_key: str) -> None:
    """
    Sube un archivo local a una clave arbitraria del Data Lake.
    Generica y parametrizada: sirve para las tres zonas Medallion.
    """
    logger = get_run_logger()
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"Archivo local no encontrado: {local_path}")

    s3 = get_s3_client()
    try:
        s3.upload_file(local_path, BUCKET, s3_key)
        logger.info(f"Cargado a s3://{BUCKET}/{s3_key}")
    except (ClientError, S3UploadFailedError) as e:
        # upload_file envuelve el ClientError en S3UploadFailedError, que NO
        # hereda de ClientError: por eso deben capturarse ambos tipos.
        logger.error(f"Fallo de credenciales o permisos en S3: {e}")
        raise


@task(name="Capa Optimized - Parquet")
def build_optimized(df: pd.DataFrame) -> str:
    """
    Convierte el DataFrame validado a Parquet (formato columnar).
    Solo recibe datos que ya superaron validate_quality.
    """
    logger = get_run_logger()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_parquet(OPTIMIZED_PARQUET, engine="pyarrow", index=False)

    tam_csv = os.path.getsize(RAW_CSV)
    tam_pq = os.path.getsize(OPTIMIZED_PARQUET)
    reduccion = (1 - tam_pq / tam_csv) * 100
    logger.info(f"Parquet generado: {OPTIMIZED_PARQUET} ({tam_pq} bytes)")
    logger.info(f"Reduccion frente al CSV: {reduccion:.1f}%")
    return OPTIMIZED_PARQUET


def _parse_categories(valor) -> list:
    """
    Reconstruye la lista de categorias desde su representacion textual.
    Se usa ast.literal_eval y no eval: literal_eval solo admite literales
    de Python, por lo que no puede ejecutar codigo arbitrario procedente
    de una fuente externa (principio de entrada no confiable).
    """
    # Caso 1: cadena de texto (dato procedente de un CSV) -> parseo seguro.
    if isinstance(valor, str):
        try:
            parsed = ast.literal_eval(valor)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, SyntaxError):
            return []
    # Caso 2: estructura nativa. Al leer Parquet, pyarrow devuelve
    # numpy.ndarray y no list, por lo que se normaliza con list().
    if valor is None:
        return []
    try:
        return list(valor)
    except TypeError:
        return []


@task(name="Capa Consumption - KPIs por categoria")
def build_consumption(parquet_path: str) -> str:
    """
    Genera la tabla analitica agregada por categoria.
    Estrategia (opcion B): explosion uno-a-muchos. Un establecimiento con
    varias categorias aporta una fila por cada una, de modo que total_places
    cuenta ASIGNACIONES categoria-establecimiento, no establecimientos.
    Por eso se reporta tambien unique_places con COUNT(DISTINCT name).
    """
    logger = get_run_logger()

    df = pd.read_parquet(parquet_path)
    df["cat_list"] = df["categories"].apply(_parse_categories)

    # Se extrae solo el nombre de cada categoria y se explota la relacion
    df["cat_name"] = df["cat_list"].apply(
        lambda lst: [c.get("name") for c in lst if isinstance(c, dict)]
    )
    df_exp = df[["name", "distance", "cat_name"]].explode("cat_name")
    df_exp = df_exp[df_exp["cat_name"].notna()]

    logger.info(f"Establecimientos unicos: {df['name'].nunique()}")
    logger.info(f"Asignaciones categoria-establecimiento: {len(df_exp)}")
    logger.info(f"Categorias distintas: {df_exp['cat_name'].nunique()}")

    # Agregacion en DuckDB: motor SQL embebido, sin servidor ni conexion
    con = duckdb.connect()
    con.register("places_exploded", df_exp)
    kpis = con.execute(
        """
        SELECT
            cat_name                        AS category,
            COUNT(*)                        AS total_places,
            COUNT(DISTINCT name)            AS unique_places,
            ROUND(AVG(distance), 2)         AS avg_distance_m,
            MIN(distance)                   AS min_distance_m,
            MAX(distance)                   AS max_distance_m
        FROM places_exploded
        GROUP BY cat_name
        ORDER BY total_places DESC, avg_distance_m ASC
        """
    ).df()
    con.close()

    kpis.to_parquet(CONSUMPTION_PARQUET, engine="pyarrow", index=False)
    logger.info(f"KPIs generados: {CONSUMPTION_PARQUET} "
                f"({os.path.getsize(CONSUMPTION_PARQUET)} bytes)")
    print("\n--- Tabla de KPIs por categoria (top 10) ---")
    print(kpis.head(10).to_string(index=False))
    return CONSUMPTION_PARQUET


@task(name="Verificacion FinOps de zonas S3")
def verify_zones() -> None:
    """
    Punto 4 del enunciado: verifica con list_objects_v2 el tamano en bytes
    de los objetos en cada zona Medallion, evidenciando empiricamente el
    beneficio de compresion del formato columnar frente al CSV crudo.
    """
    logger = get_run_logger()
    s3 = get_s3_client()

    try:
        resp = s3.list_objects_v2(Bucket=BUCKET)
    except ClientError as e:
        logger.error(f"No fue posible listar el bucket: {e}")
        raise

    objetos = resp.get("Contents", [])
    if not objetos:
        logger.warning("El bucket no contiene objetos.")
        return

    base = next((o["Size"] for o in objetos
                 if o["Key"] == KEY_RAW), None)

    print("\n--- Verificacion de zonas del Data Lake ---")
    print(f"{'Zona / Clave':<55} {'Bytes':>10} {'% vs CSV':>10}")
    print("-" * 77)
    for o in sorted(objetos, key=lambda x: x["Key"]):
        rel = f"{o['Size'] / base * 100:.1f}%" if base else "n/a"
        print(f"{o['Key']:<55} {o['Size']:>10} {rel:>10}")
    print("-" * 77)
    logger.info(f"Objetos verificados en el Data Lake: {len(objetos)}")


@flow(name="Pipeline Medallion HCN", log_prints=True)
def main_flow() -> int:
    """
    Orquestador principal. Encadena las etapas del pipeline y aplica
    degradacion controlada: ante cualquier fallo se registra un mensaje
    descriptivo y el proceso termina con un codigo de salida distinguible.
    """
    logger = get_run_logger()

    if not BUCKET:
        logger.error("S3_BUCKET_NAME no definido en .env")
        return 1

    try:
        # --- Zona Raw ---
        df = extract_places()
        csv_path = save_raw_csv(df)
        upload_to_s3(csv_path, KEY_RAW)

        # --- Validacion previa a la zona Optimized ---
        df_validado = validate_quality(df)

        # --- Zona Optimized ---
        parquet_path = build_optimized(df_validado)
        upload_to_s3(parquet_path, KEY_OPTIMIZED)

        # --- Zona Consumption ---
        kpis_path = build_consumption(parquet_path)
        upload_to_s3(kpis_path, KEY_CONSUMPTION)

        # --- Verificacion FinOps ---
        verify_zones()

        logger.info("Pipeline completado correctamente.")
        return 0

    except DataQualityError as e:
        # No es un fallo tecnico: el pipeline funciono y rechazo datos malos
        logger.error(f"CALIDAD DE DATOS: {e}")
        return 2
    except (ClientError, S3UploadFailedError, FileNotFoundError,
            requests.exceptions.RequestException) as e:
        logger.error(f"FALLO OPERATIVO: {type(e).__name__}: {e}")
        return 1
    except Exception as e:
        logger.error(f"FALLO NO PREVISTO: {type(e).__name__}: {e}")
        return 1


# El codigo de salida se traduce fuera del flujo: main_flow decide QUE paso
# (estado Failed en Prefect) y el punto de entrada lo traduce a exit code.
if __name__ == "__main__":
    sys.exit(main_flow())
