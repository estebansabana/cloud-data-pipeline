import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load temporary AWS credentials from .env
load_dotenv()

# Create the S3 client using the temporary session credentials
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
)

bucket_name = "esteban-datalake-hcn-2026"          # Change to your bucket name
local_file = "output/restaurants_raw.csv"
path_s3 = "raw_zone/restaurants_raw.csv"  # "Folders" in S3 are key prefixes

# Check that the local file exists before uploading
if not os.path.isfile(local_file):
    raise FileNotFoundError(
        f"Local file not found: {local_file}. Run data_ingestion.py first."
    )

# Upload the file to the raw zone of the Data Lake
try:
    s3_client.upload_file(local_file, bucket_name, path_s3)
    print(f"File successfully uploaded to s3://{bucket_name}/{path_s3}")
except ClientError as e:
    print(f"Credentials or permissions error: {e}")