from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="sales_ml_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    train = BashOperator(
        task_id="train_models",
        bash_command="python3 engineering/train.py",
    )

    predict = BashOperator(
        task_id="predict_sales",
        bash_command="python3 engineering/predict.py",
    )

    train >> predict
