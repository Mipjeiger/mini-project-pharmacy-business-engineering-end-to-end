import pandas as pd
import joblib
from sqlalchemy import create_engine
from minio import Minio
import io
import os
from dotenv import load_dotenv

# load environment variables
env_path = os.path.join("..", ".env")
load_dotenv(dotenv_path=env_path)

# Retrieve database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)