from dagster import ConfigurableResource, EnvVar
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd


class SnowflakeDB(ConfigurableResource):
    account: str = EnvVar("SNOWFLAKE_ACCOUNT")
    username: str = EnvVar("SNOWFLAKE_USERNAME")
    password: str = EnvVar("SNOWFLAKE_PASSWORD")
    database: str = EnvVar("SNOWFLAKE_DB")
    schema_: str = "PUBLIC"
    warehouse: str = EnvVar("SNOWFLAKE_WAREHOUSE")
    role: str = EnvVar("SNOWFLAKE_ROLE")
    RESERVED_COLUMNS: dict = { "by": "BY", "type": "TYPE" }

    def get_connection(self): 
        """Return a Snowflake connector connection.""" 
        return snowflake.connector.connect( 
            user=self.username, 
            password=self.password, 
            account=self.account, 
            warehouse=self.warehouse, 
            database=self.database, 
            schema=self.schema_, 
            role=self.role, 
            )

    def execute(self, sql: str): 
        """Execute a raw SQL query and return results.""" 
        conn = self.get_connection() 
        try: 
            cur = conn.cursor() 
            cur.execute(sql) 
            results = cur.fetchall() 
            return results 
        finally: 
            cur.close() 
            conn.close()

    def write_dataframe(self, df: pd.DataFrame, table_name: str):
        """
        Write a Pandas DataFrame to Snowflake.
        Handles ARRAY columns if they are lists in Python.
        """
        # Convert any list columns to OBJECT for Snowflake
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: x if isinstance(x, list) else None)

        df = df.rename(columns={k: v for k, v in self.RESERVED_COLUMNS.items() if k in df.columns})
        df.columns = [col.upper() for col in df.columns]

        conn = self.get_connection()
        try:
            success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
            if not success:
                raise RuntimeError(f"Failed to write DataFrame to {table_name}")
            return nrows
        finally:
            conn.close()
