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
        pool_size=3,  # Further reduced
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

    ensure_buckets_exists()

    project_root, _ = get_env_path()
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

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
            limit=1000, max_batches=10, group_id="airflow_sales_pipeline"
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
    """Create features and save to PostgreSQL - OPTIMIZED TO AVOID OOM"""
    from sqlalchemy import text
    import time

    minio = get_minio_client()
    engine = get_db_engine()

    try:
        objects = list(minio.list_objects("silver", prefix="pharmacy_sales_"))
        if not objects:
            print("No data in silver bucket")
            return

        print(f"Found {len(objects)} files in silver bucket")

        # Test database connection first
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

        # CRITICAL: Process each file chunk independently
        # Don't combine all - write directly to DB and MinIO
        FILE_CHUNK_SIZE = 2  # Process 2 files at a time (reduced from 3)
        total_chunks = (len(objects) + FILE_CHUNK_SIZE - 1) // FILE_CHUNK_SIZE
        is_first_write = True
        all_chunk_files = []
        total_records = 0

        for file_chunk_idx in range(0, len(objects), FILE_CHUNK_SIZE):
            chunk_num = (file_chunk_idx // FILE_CHUNK_SIZE) + 1
            chunk_objects = objects[file_chunk_idx : file_chunk_idx + FILE_CHUNK_SIZE]

            print(f"\n{'='*60}")
            print(f"Processing chunk {chunk_num}/{total_chunks}")
            print(f"{'='*60}")

            # Load chunk files
            dfs = []
            for obj in chunk_objects:
                try:
                    response = minio.get_object("silver", obj.object_name)
                    dfs.append(pd.read_parquet(io.BytesIO(response.read())))
                    response.close()
                    response.release_conn()
                    print(f"  ✓ Loaded {obj.object_name}")
                except Exception as e:
                    print(f"  ✗ Error reading {obj.object_name}: {e}")
                    raise

            if not dfs:
                continue

            df = pd.concat(dfs, ignore_index=True)
            print(f"  Chunk records: {len(df)}")
            del dfs
            gc.collect()

            # Aggregate features
            print("  Creating features...")
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

            del df
            gc.collect()

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
            print("  Engineering features...")
            features = features.sort_values(["distributor", "city", "year", "month"])
            grp = features.groupby(["distributor", "product_name", "city"])

            # Lag features
            features["lag_1m_sales"] = grp["total_sales_clean"].shift(1)
            features["lag_3m_sales"] = grp["total_sales_clean"].shift(3)
            features["lag_6m_sales"] = grp["total_sales_clean"].shift(6)

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

            del grp
            gc.collect()

            print(f"  Chunk features: {len(features)} records")

            # Save chunk to MinIO gold bucket
            chunk_filename = f"features_chunk_{chunk_num}.parquet"
            buffer = io.BytesIO()
            features.to_parquet(buffer, index=False)
            buffer.seek(0)
            minio.put_object("gold", chunk_filename, buffer, len(buffer.getbuffer()))
            print(f"  ✓ Saved to gold/{chunk_filename}")
            all_chunk_files.append(chunk_filename)
            del buffer
            gc.collect()

            # WRITE DIRECTLY TO DATABASE (chunk by chunk, don't combine!)
            print(f"  Writing {len(features)} records to database...")
            batch_size = 150  # Very small batches
            total_batches = (len(features) + batch_size - 1) // batch_size

            for i in range(0, len(features), batch_size):
                batch_num = i // batch_size + 1
                batch = features.iloc[i : i + batch_size].copy()

                try:
                    batch.to_sql(
                        "sales_feature",
                        engine,
                        schema="features",
                        if_exists="replace" if is_first_write else "append",
                        index=False,
                        method=None,
                        chunksize=50,
                    )

                    # Only first batch of first chunk uses 'replace'
                    if is_first_write:
                        is_first_write = False
                        print(f"    ✓ Created table and wrote batch 1")
                    else:
                        print(f"    ✓ Batch {batch_num}/{total_batches}", end="\r")

                except Exception as e:
                    print(f"\n    ✗ Error writing batch {batch_num}: {e}")
                    raise
                finally:
                    del batch
                    gc.collect()

            print(f"\n  ✓ Chunk {chunk_num} written to database")
            total_records += len(features)

            # Delete features immediately after writing
            del features
            gc.collect()

        print(f"\n{'='*60}")
        print("Creating final combined parquet and analytics...")
        print(f"{'='*60}")

        # Load chunks ONE BY ONE and combine (if needed for final parquet)
        print("Combining chunks for final parquet...")
        final_features = []
        for chunk_file in all_chunk_files:
            response = minio.get_object("gold", chunk_file)
            chunk_df = pd.read_parquet(io.BytesIO(response.read()))
            final_features.append(chunk_df)
            response.close()
            response.release_conn()
            print(f"  ✓ Loaded {chunk_file}")

            # If accumulated enough, save intermediate and clear
            if len(final_features) >= 3:
                print("  Saving intermediate combined chunk...")
                combined = pd.concat(final_features, ignore_index=True)

                # Save intermediate
                buffer = io.BytesIO()
                combined.to_parquet(buffer, index=False)
                buffer.seek(0)
                intermediate_file = f"intermediate_{len(final_features)}.parquet"
                minio.put_object(
                    "gold", intermediate_file, buffer, len(buffer.getbuffer())
                )

                del buffer, combined, final_features
                gc.collect()
                final_features = [
                    pd.read_parquet(
                        io.BytesIO(minio.get_object("gold", intermediate_file).read())
                    )
                ]

        # Final combine
        features = pd.concat(final_features, ignore_index=True)
        print(f"Total combined: {len(features)} records")
        del final_features
        gc.collect()

        # Save final
        buffer = io.BytesIO()
        features.to_parquet(buffer, index=False)
        buffer.seek(0)
        gold_filename = (
            f"sales_feature_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        )
        minio.put_object("gold", gold_filename, buffer, len(buffer.getbuffer()))
        print(f"✓ Saved final to gold/{gold_filename}")
        del buffer
        gc.collect()

        # Analytics (process in smaller batches)
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
        print(f"✓ Saved to analytics/{analytics_filename} ({len(analytics)} records)")

        del buffer_analytics, analytics, features
        gc.collect()

        # Verify database
        print("Verifying database...")
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM features.sales_feature")
                )
                count = result.scalar()
                print(f"✓ Database has {count} records")

        except Exception as e:
            print(f"✗ Verification error: {e}")

        print("\n✅ silver_to_gold completed successfully!")

    except Exception as e:
        print(f"\n❌ Error in silver_to_gold: {e}")
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
        "retries": 1,
        "retry_delay": 60,
    },
) as dag:

    t1 = PythonOperator(task_id="kafka_to_bronze", python_callable=kafka_to_bronze)
    t2 = PythonOperator(task_id="bronze_to_silver", python_callable=bronze_to_silver)
    t3 = PythonOperator(task_id="silver_to_gold", python_callable=silver_to_gold)

    t1 >> t2 >> t3
