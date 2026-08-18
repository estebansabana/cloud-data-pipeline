import os
import io
import duckdb
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load temporary AWS credentials from .env
load_dotenv()

# Create the S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
)

bucket_name = "esteban-datalake-hcn-2026"           # Change to your bucket name
source_key = "raw_zone/restaurants_raw.csv"

# --- Download the S3 object into memory (no local disk write) ---
try:
    obj = s3_client.get_object(Bucket=bucket_name, Key=source_key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    print(df.info())
except ClientError as e:
    raise SystemExit(f"Error reading the object from S3: {e}")

# --- Convert to Parquet for optimized (columnar) storage ---
df.to_parquet("output/restaurants_processed.parquet", engine="pyarrow", index=False)
print("File transformed and saved as restaurants_processed.parquet")

# --- High-speed analytics with DuckDB ---
con = duckdb.connect()
result = con.execute(
    """
    SELECT name, distance
    FROM 'output/restaurants_processed.parquet'
    WHERE distance < 500
    ORDER BY distance ASC
    """
).df()

print("Analytical result (restaurants closer than 500 m):")
print(result)
con.close()
