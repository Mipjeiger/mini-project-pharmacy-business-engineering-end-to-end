"""
Sales Feature Pipeline - Bronze -> Silver -> Gold
Kafka to MinIO to PostgreSQL data pipeline
"""

import io
import os
import pandas as pd
import numpy as np
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import gc  # Garbage collector


# Function to get project root and .env path
def get_env_path():
    """Get project root and .env path"""
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return root, os.path.join(root, ".env")


# Function to create MinIO client
def get_minio_client():
    """Create MinIO client - called inside each task"""
    from minio import Minio
    from dotenv import load_dotenv

    project_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    env_path = os.path.join(project_root, ".env")
    load_dotenv(dotenv_path=env_path)

    return Minio(
        f"{os.getenv('MINIO_HOST')}:{os.getenv('MINIO_PORT')}",
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )


# Function to call PostgreSQL engine
def get_db_engine():
    """Create PostgreSQL engine"""
    from sqlalchemy import create_engine
    from dotenv import load_dotenv

    _, env_path = get_env_path()
    load_dotenv(dotenv_path=env_path)

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Database environment variables are not fully set.")

    db_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=5,  # Limit connection pool
        max_overflow=0,
    )


# Ensure minio bucket exist
def ensure_buckets_exists():
    """Ensure all required buckets exists"""
    minio = get_minio_client()
    required_buckets = ["bronze", "silver", "gold", "analytics"]

    for bucket in required_buckets:
        try:
            if not minio.bucket_exists(bucket):
                minio.make_bucket(bucket)
                print(f"Created bucket: {bucket}")
            else:
                print(f"Bucket exists: {bucket}")
        except Exception as e:
            print(f"Error checking/creating bucket {bucket}: {e}")


def kafka_to_bronze():
    """Consume Kafka messages and save to MinIO bronze bucket"""
    import sys
    import importlib.util

    # Ensure buckets exist
    ensure_buckets_exists()

    project_root, _ = get_env_path()
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Load consumer module dynamically to avoid kafka package conflict
    consumer_path = os.path.join(project_root, "kafka", "consumer.py")

    if not os.path.exists(consumer_path):
        raise FileNotFoundError(f"Kafka consumer module not found at {consumer_path}")

    spec = importlib.util.spec_from_file_location("kafka_consumer", consumer_path)
    consumer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(consumer)

    minio = get_minio_client()
    count = 0

    print("Reading from Kafka...")
    try:
        for batch in consumer.read_batch(
            limit=1000, max_batches=5, group_id="airflow_sales_pipeline"
        ):
            if not batch:
                continue

            df = pd.DataFrame(batch)
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            filename = f"pharmacy_sales_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{count}.parquet"
            minio.put_object("bronze", filename, buffer, len(buffer.getbuffer()))
            print(f"✓ {filename}: {len(df)} records")
            count += 1

            # Free memory
            del df, buffer
            gc.collect()

        print(f"✓ Created {count} files in bronze")

    except Exception as e:
        print(f"Error in kafka_to_bronze: {e}")
        raise


def bronze_to_silver():
    """Clean bronze data and save to silver bucket"""
    minio = get_minio_client()
    try:
        objects = list(minio.list_objects("bronze", prefix="pharmacy_sales_"))

        if not objects:
            print("No files found in bronze bucket.")
            return

        print(f"Processing {len(objects)} files...")
        for obj in objects:
            try:
                response = minio.get_object("bronze", obj.object_name)
                df = pd.read_parquet(io.BytesIO(response.read()))
                response.close()
                response.release_conn()

                # Clean negative sales to prevent skewed analysis
                if "sales" in df.columns:
                    df = df[df["sales"] >= 0]
                else:
                    print(f"Warning: 'sales' column not found in {obj.object_name}")

                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)

                minio.put_object(
                    "silver", obj.object_name, buffer, len(buffer.getbuffer())
                )
                print(f"✓ {obj.object_name}: {len(df)} records")

                # Free memory
                del df, buffer
                gc.collect()

            except Exception as e:
                print(f"Error processing {obj.object_name}: {e}")
                raise

        print(f"✓ Processed {len(objects)} files to silver bucket.")
    except Exception as e:
        print(f"Error in bronze_to_silver: {e}")
        raise


def silver_to_gold():
    """Create features and save to PostgreSQL"""
    from sqlalchemy import text
    import time

    minio = get_minio_client()
    engine = get_db_engine()

    try:
        objects = list(minio.list_objects("silver", prefix="pharmacy_sales_"))
        if not objects:
            print("No data in silver bucket")
            return

        print(f"Loading {len(objects)} files...")

        # Load and combine data
        dfs = []
        for obj in objects:
            try:
                response = minio.get_object("silver", obj.object_name)
                dfs.append(pd.read_parquet(io.BytesIO(response.read())))
                response.close()
                response.release_conn()
            except Exception as e:
                print(f"Error reading {obj.object_name}: {e}")
                raise

        df = pd.concat(dfs, ignore_index=True)
        print(f"Total: {len(df)} records")
        del dfs
        gc.collect()

        # Aggregate feature
        print("Creating features...")
        features = (
            df.groupby(
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
            )
            .agg(
                total_quantity=("quantity", "sum"),
                total_sales=("sales", "sum"),
                avg_price=("price", "mean"),
            )
            .reset_index()
        )

        # Clean outliers
        sales_upper_bound = features["total_sales"].quantile(0.95)
        features["total_sales_clean"] = features["total_sales"].clip(
            lower=50, upper=sales_upper_bound
        )

        # Convert month to digit
        month_mapping = {
            "January": 1,
            "February": 2,
            "March": 3,
            "April": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "August": 8,
            "September": 9,
            "October": 10,
            "November": 11,
            "December": 12,
        }

        features["year"] = features["year"].astype(int)
        features["month"] = features["month"].map(month_mapping)
        features = features.sort_values(by=["distributor", "year", "month"])

        # Feature engineering
        print("Engineering features...")
        features = features.sort_values(["distributor", "city", "year", "month"])
        grp = features.groupby(["distributor", "product_name", "city"])

        # Lag features
        features["lag_1m_sales"] = grp["total_sales_clean"].shift(1)
        features["lag_3m_sales"] = grp["total_sales_clean"].shift(3)
        features["lag_6m_sales"] = grp["total_sales_clean"].shift(6)
        print("✓ Created lag features")

        # Rolling features
        features["rolling_avg_3m"] = grp["total_sales_clean"].transform(
            lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
        )
        features["rolling_avg_6m"] = grp["total_sales_clean"].transform(
            lambda x: x.shift(1).rolling(window=6, min_periods=1).mean()
        )

        # Growth percentage
        features["sales_growth_pct"] = grp["total_sales_clean"].transform(
            lambda x: x.pct_change().shift(1) * 100
        )

        # Seasonal features
        features["month_sin"] = np.sin(2 * np.pi * features["month"] / 12)
        features["month_cos"] = np.cos(2 * np.pi * features["month"] / 12)

        # Clean NaN
        features = features.replace([np.inf, -np.inf], np.nan)
        features = features.fillna(0)

        # Delete df to free memory
        del df, grp
        gc.collect()

        # Save to MinIO gold bucket
        print(f"Saving {len(features)} records to gold bucket...")
        buffer = io.BytesIO()
        features.to_parquet(buffer, index=False)
        buffer.seek(0)

        gold_filename = (
            f"sales_feature_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        minio.put_object("gold", gold_filename, buffer, len(buffer.getbuffer()))
        print(f"✓ Saved to gold/{gold_filename}")
        del buffer
        gc.collect()

        # Save analytics version
        print("Creating analytics summary...")
        analytics = (
            features.groupby(["distributor", "city", "year", "month"])
            .agg(
                total_quantity=("total_quantity", "sum"),
                total_sales=("total_sales", "sum"),
                avg_price=("avg_price", "mean"),
                product_count=("product_name", "nunique"),
                sales_growth_pct_avg=("sales_growth_pct", "mean"),
            )
            .reset_index()
        )

        buffer_analytics = io.BytesIO()
        analytics.to_parquet(buffer_analytics, index=False)
        buffer_analytics.seek(0)

        analytics_filename = (
            f"sales_analytics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        minio.put_object(
            "analytics",
            analytics_filename,
            buffer_analytics,
            len(buffer_analytics.getbuffer()),
        )
        print(
            f"✓ Saved to analytics/{analytics_filename} ({len(analytics)} summary records)"
        )
        del buffer_analytics, analytics
        gc.collect()

        # Test database connection
        try:
            with engine.connect() as test_conn:
                test_conn.execute(text("SELECT 1"))
                print("✓ Database connection successful")
        except Exception as e:
            print(f"Database connection failed: {e}")
            raise

        # Ensure schema exists
        try:
            with engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS features"))
                conn.commit()
                print("✓ Schema 'features' ready")
        except Exception as e:
            print(f"Warning: Could not create schema: {e}")

        # Write to database with MEMORY OPTIMIZATION
        print(f"Writing {len(features)} records to database...")

        # SMALLER batch size to reduce memory
        batch_size = 300  # Reduced from 800
        total_batches = (len(features) + batch_size - 1) // batch_size

        for i in range(0, len(features), batch_size):
            batch_num = i // batch_size + 1
            batch = features.iloc[
                i : i + batch_size
            ].copy()  # Use copy to avoid warnings

            print(
                f"  Writing batch {batch_num}/{total_batches} ({len(batch)} records)..."
            )

            try:
                start_time = time.time()

                # Use method=None instead of 'multi' to save memory
                batch.to_sql(
                    "sales_feature",
                    engine,
                    schema="features",
                    if_exists="replace" if i == 0 else "append",
                    index=False,
                    method=None,  # Changed from 'multi' - uses less memory
                    chunksize=50,  # Write in very small chunks
                )

                elapsed = time.time() - start_time
                print(f"    ✓ Batch {batch_num} done in {elapsed:.2f}s")

            except Exception as e:
                print(f"    ✗ Error writing batch {batch_num}: {e}")
                raise
            finally:
                del batch
                gc.collect()  # Force garbage collection after each batch

        # Verify
        print("Verifying database...")
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM features.sales_feature")
                )
                count = result.scalar()
                print(f"✓ Verified: {count} records in database")

                if count != len(features):
                    print(f"⚠️  Warning: Expected {len(features)}, got {count}")

        except Exception as e:
            print(f"✗ Verification error: {e}")

        print("✓ silver_to_gold completed successfully")

    except Exception as e:
        print(f"Error in silver_to_gold: {e}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        print("Cleaning up...")
        engine.dispose()
        gc.collect()


# DAG definition
with DAG(
    dag_id="sales_feature_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pharmacy", "etl"],
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": 60,
        # Add memory limits if using KubernetesExecutor
    },
) as dag:

    t1 = PythonOperator(task_id="kafka_to_bronze", python_callable=kafka_to_bronze)
    t2 = PythonOperator(task_id="bronze_to_silver", python_callable=bronze_to_silver)
    t3 = PythonOperator(task_id="silver_to_gold", python_callable=silver_to_gold)

    t1 >> t2 >> t3
