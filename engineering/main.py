import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
import minio
from urllib3 import PoolManager
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# Create database engine
def get_db_engine():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    # Validate environment variables
    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Database environment variables are not fully set.")

    return create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


# Read parqued file from MinIO
def read_parquet_from_minio(minio_client, bucket_name, object_name):
    try:
        response = minio_client.get_object(bucket_name, object_name)

        # Read data to BytesIO
        data = BytesIO(response.read())  # BytesIO is needed for read data in memory

        # Close the response to free up resources
        response.close()
        response.release_conn()

        # Read parquet file into DataFrame
        df = pd.read_parquet(data)
        return df

    except Exception as e:
        print(f"Error reading parquet file from MinIO: {e}")
        raise


# Get all parquet files from a MinIO bucket
def get_all_parquet_files(minio_client):
    all_data = {}

    try:
        # List all buckets
        buckets = minio_client.list_buckets()
        print(f"Found buckets: {[bucket.name for bucket in buckets]}")

        for bucket in buckets:
            bucket_name = bucket.name
            print(f"Processing bucket: {bucket_name}")
            all_data[bucket_name] = {}

            # List all objects in the bucket
            objects = minio_client.list_objects(bucket_name, recursive=True)

            for obj in objects:
                if obj.object_name.endswith(".parquet"):
                    print(f"Reading object: {obj.object_name}")
                    try:
                        df = read_parquet_from_minio(
                            minio_client=minio_client,
                            bucket_name=bucket_name,
                            object_name=obj.object_name,
                        )
                        all_data[bucket_name][obj.object_name] = df
                        print(f"Shape: {df.shape} for object: {obj.object_name}")
                    except Exception as e:
                        print(
                            f"Failed to read {obj.object_name} from bucket {bucket_name}: {e}"
                        )

        return all_data

    except Exception as e:
        print(f"Error retrieving parquet files from MinIO: {e}")
        raise


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

        # retrieve all parquet files
        all_data = get_all_parquet_files(minio_client=minio_client)

        # Show summary of data retrieved
        for bucket_name, objects in all_data.items():
            print(f"Bucket: {bucket_name}")
            print(f"Total files retrieved: {len(objects)}")
            for object_name, df in objects.items():
                print(f" - {object_name}: {df.shape} ")

    except Exception as e:
        print(f"Failed to connect to MinIO or retrieve data: {e}")
