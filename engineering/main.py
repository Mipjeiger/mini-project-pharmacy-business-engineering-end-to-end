import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
import minio
from urllib3 import PoolManager
from io import BytesIO
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=env_path)


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


# Read data parquet
def read_parquet(file_path: str) -> pd.DataFrame:
    return pd.read_parquet(file_path)


# Callable example
if __name__ == "__main__":
    print("Setting up database engine...")

    try:
        engine = get_db_engine()
        print("Database engine created.")
    except Exception as e:
        print(f"Failed to create database engine: {e}")

    print("Reading data from Parquet file...")
    try:
        df = read_parquet("data/sales_feature_20260105_130925.parquet")
        print("Data read from Parquet file:")
        print(df)
    except Exception as e:
        print(f"Failed to read data from Parquet file: {e}")
