import streamlit as st
import pandas as pd
from minio import Minio
from dotenv import load_dotenv
import os

env_path = os.path.join("..", ".env")
load_dotenv(dotenv_path=env_path)

st.set_page_config(layout="wide")

minio = Minio(
    mini_host=os.getenv("minio_host"),
    mini_port=int(os.getenv("minio_port")),
    access_key=os.getenv("access_key"),
    secret_key=os.getenv("secret_key"),
    bucket_name=os.getenv("bucket_name"),
    secure=False,
)

data = minio.get_object(
    "analytics",
    "sales_prediction/sales_prediction.parquet",
)

df = pd.read_parquet(data)
st.title("Pharmacy Sales Prediction Dashboard")
st.metric(
    "Total Predicted Sales",
    f"{df.predicted_sales.sum():,.2f}",
)

st.dataframe(df)
