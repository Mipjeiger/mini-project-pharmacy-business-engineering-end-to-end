import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
import minio


# Create database connection
def get_db_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}",
        poll_pre_ping=True,
    )


# Read parqued file from MinIO
def read_parquet_from_minio(minio_client, bucket_name, object_name):
    response = minio_client.get_object(bucket_name, object_name)
    return pd.read_parquet(response)


# Callable example
if __name__ == "__main__":
    print("Setting up database engine...")

    try:
        engine = get_db_engine()
        print("Database engine created.")
    except Exception as e:
        print(f"Failed to create database engine: {e}")

    print("Connecting to MinIO...")
    try:
        minio_client = minio.Minio(
            f"{os.getenv('minio_host')}:{os.getenv('minio_port')}",
            access_key=os.getenv("access_key"),
            secret_key=os.getenv("secret_key"),
            secure=False,
        )
        print("MinIO client created.")
    except Exception as e:
        print(f"Failed to create MinIO client: {e}")
