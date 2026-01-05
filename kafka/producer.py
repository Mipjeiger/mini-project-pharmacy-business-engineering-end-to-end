import json
import psycopg2
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

# initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9095",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Load environment variables from .env file
env_path = os.path.join("..", ".env")
load_dotenv(dotenv_path=env_path)

# Retrieve database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Use psycopg2 directly to connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
)


# Fetch data from the database and send to Kafka topic
def send_data_to_kafka():
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM raw.pharmacy_sales;")
        cols = [c[0] for c in cur.description]

        for row in cur.fetchall():
            producer.send("pharmacy_sales", dict(zip(cols, row)))

        producer.flush()
        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    send_data_to_kafka()
