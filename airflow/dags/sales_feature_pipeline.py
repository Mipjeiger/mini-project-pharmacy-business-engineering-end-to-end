from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import io
import numpy as np
import json
from minio import Minio
from kafka.consumer import read_batch
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# load environment variables
env_path = os.path.join("..", ".env")
load_dotenv(dotenv_path=env_path)

# Retrieve database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

minio = Minio(
    minio_host=os.getenv("minio_host"),
    minio_port=int(os.getenv("minio_port")),
    access_key=os.getenv("access_key"),
    secret_key=os.getenv("secret_key"),
    bucket_name=os.getenv("bucket_name"),
    secure=False,
)

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


# function in processing kafka data to kafka bronze topic involving minio as storage
def kafka_to_bronze():
    for batch in read_batch(limit=1000):
        df = pd.DataFrame(batch)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)

        minio.put_object(
            "bronze",
            f"pharmacy_sales_{datetime.now().isoformat()}.parquet",
            buffer,
            buffer.getbuffer().nbytes,
        )


# function in processing bronze data to silver data in postgres
def bronze_to_silver():
    objects = minio.list_objects("bronze", prefix="pharmacy_sales_")
    for obj in objects:
        df = df[df.sales >= 0]  # cleaning negative sales
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)

        minio.put_object(
            "silver",
            obj.object_name,
            buffer,
            buffer.getbuffer().nbytes,
        )


# function in processing silver data to gold data in postgres
def silver_to_gold():
    data = []
    for obj in minio.list_objects("silver", prefix="pharmacy_sales_"):
        data.append(pd.read_parquet(minio.get_object("silver", obj.object_name)))

    df = pd.concat(data)

    # create feature
    feature = df.groupby(
        [
            "distributor",
            "channel",
            "sub_channel",
            "city",
            "product_name",
            "product_class",
            "sales_team",
            "year",
            "month",
        ]
    ).agg(
        total_quantity=("quantity", "sum"),
        total_sales=("sales", "sum"),
        avg_price=("price", "mean"),
        rolling_avg_3m_sales=(
            "sales",
            lambda x: np.mean(x.rolling(window=3).mean() if len(x) >= 3 else np.nan),
        ),
        sales_growth_pct=(
            "sales",
            lambda x: (
                (x.iloc[-1] - x.iloc[0]) / x.iloc[0] * 100
                if len(x) > 1 and x.iloc[0] != 0
                else np.nan
            ),
        ).reset_index(),
    )

    feature.to_sql(
        "sales_feature",
        engine,
        schema="features",
        if_exists="replace",
        index=False,
    )


with DAG(
    dag_id="sales_feature_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sales", "features"],
) as dag:

    t1 = PythonOperator(task_id="kafka_to_bronze", python_callable=kafka_to_bronze)
    t2 = PythonOperator(task_id="bronze_to_silver", python_callable=bronze_to_silver)
    t3 = PythonOperator(task_id="silver_to_gold", python_callable=silver_to_gold)

    t1 >> t2 >> t3
