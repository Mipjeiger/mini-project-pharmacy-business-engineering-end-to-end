import duckdb
from dotenv import load_dotenv
import os

con = duckdb.connect()

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Set minio credentials
con.execute(
    """
            SET s3_endpoint='localhost:9000';
            set s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            """
)


print("DuckDB MinIO configuration set.")

# Load and execute SQL file
if __name__ == "__main__":
    sql_file = os.path.join(os.path.dirname(__file__), "duckDB.sql")

    if os.path.exists(sql_file):
        print(f"Executing SQL file: {sql_file}")
        with open(sql_file, "r") as f:
            sql_commands = f.read()

        # Excute the SQL commands
        con.execute(sql_commands)
        print("SQL file executed successfully.")
    else:
        print(f"SQL file not found: {sql_file}")

    # Keep connection open for queries
    print("DuckDB connection is ready for queries.")
