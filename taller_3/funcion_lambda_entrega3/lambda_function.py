import boto3
import csv
import io
import ast
import json
import os
import pg8000.dbapi


def lambda_handler(event, context):
    conn = None
    cur = None

    try:
        # 1. Configuración
        bucket = "anderssonsarmiento"
        key = "raw_zone/restaurants_raw.csv"

        # 2. Lectura del CSV desde S3
        s3 = boto3.client("s3")
        response = s3.get_object(Bucket=bucket, Key=key)

        csv_content = response["Body"].read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(csv_content))

        # 3. Conexión a RDS PostgreSQL
        conn = pg8000.dbapi.connect(
            host=os.environ["DB_HOST"],
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"]
        )

        cur = conn.cursor()

        # 4. UPSERT parametrizado e idempotente
        UPSERT_SQL = """
        INSERT INTO datos_externos
        (name, address, categories, latitude, longitude, distance)
        VALUES (%s, %s, %s::jsonb, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET
            address = EXCLUDED.address,
            categories = EXCLUDED.categories,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            distance = EXCLUDED.distance;
        """

        records_processed = 0

        # 5. Transformación y carga
        for row in reader:
            name = row["name"].strip()

            if not name:
                raise ValueError("Se encontró un registro sin nombre.")

            address = row.get("location.address") or None

            categories_raw = row.get("categories") or "[]"

            # El CSV contiene una lista con sintaxis de Python
            categories = ast.literal_eval(categories_raw)

            categories_json = json.dumps(
                categories,
                ensure_ascii=False
            )

            latitude = float(row["latitude"])
            longitude = float(row["longitude"])
            distance = int(row["distance"])

            cur.execute(
                UPSERT_SQL,
                (
                    name,
                    address,
                    categories_json,
                    latitude,
                    longitude,
                    distance
                )
            )

            records_processed += 1

        # 6. Confirmar transacción
        conn.commit()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "ETL ejecutado correctamente.",
                "records_processed": records_processed,
                "source": f"s3://{bucket}/{key}",
                "destination": "RDS PostgreSQL - datos_externos"
            }, ensure_ascii=False)
        }

    except Exception as e:
        # Revertir cambios si ocurre algún error
        if conn:
            conn.rollback()

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error durante la ejecución del ETL.",
                "error": str(e)
            }, ensure_ascii=False)
        }

    finally:
        if cur:
            cur.close()

        if conn:
            conn.close()
