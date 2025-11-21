import os
from dagster import EnvVar
from defs.resources.postgres_db import PostgresDB
from sqlalchemy import text

def test_connection():
    # Load env vars if not set (for local testing convenience)
    # In a real scenario, these should be set in the environment
    if not os.getenv("POSTGRES_HOSTNAME"):
        print("Warning: POSTGRES_HOSTNAME not set. Using defaults/assuming env is set.")

    pg = PostgresDB(
        hostname=os.getenv("POSTGRES_HOSTNAME", "127.0.0.1"),
        username=os.getenv("POSTGRES_USERNAME", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "root"),
        database=os.getenv("POSTGRES_DB", "postgres"),
    )

    print(f"Testing connection to: {pg.connection_string.replace(pg.password, '******')}")

    try:
        engine = pg.get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM stories LIMIT 10"))
            print("Connection successful! Result:", result.fetchone())
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection()
