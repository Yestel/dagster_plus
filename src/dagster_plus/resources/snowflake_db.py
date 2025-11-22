from dagster import ConfigurableResource, EnvVar, get_dagster_logger
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd


class SnowflakeDBClient:
    def __init__(self, account, username, password, database, schema_, warehouse, role, log):
        self.account = account
        self.username = username
        self.password = password
        self.database = database
        self.schema_ = schema_
        self.warehouse = warehouse
        self.role = role
        self.log = log
        self.RESERVED_COLUMNS = { "by": "_BY", "type": "_TYPE" }

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
        self.log.info(f"Executing SQL: {sql}")
        conn = self.get_connection() 
        try: 
            cur = conn.cursor() 
            cur.execute(sql) 
            results = cur.fetchall() 
            return results 
        finally: 
            cur.close() 
            conn.close()

    def write_dataframe(self, df: pd.DataFrame, table_name: str, upsert_keys: list, extra_column_name: str = "_OTHERS"):
        """
        Write a Pandas DataFrame to a Snowflake table using upsert (MERGE) logic.
        Columns not in the target table will be stored in a JSON/VARIANT column.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            table_name (str): The name of the target table.
            upsert_keys (list): A list of column names to use as the unique key for upsert.
            extra_column_name (str): The name of the VARIANT column to store extra fields.
        """
        if not upsert_keys:
            raise ValueError("upsert_keys must be provided for upsert logic.")

        target_table = table_name.upper()
        staging_table = f"{target_table}_STG"

        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # --- Step 0: Get target table columns from Snowflake ---
            cursor.execute(f"SHOW COLUMNS IN {target_table}")
            existing_columns = [row[2].upper() for row in cursor.fetchall()]  # row[2] = column_name

            # --- Step 1: Handle extra columns not in target ---
            # Rename reserved columns and uppercase all
            df = df.rename(columns={k: v for k, v in self.RESERVED_COLUMNS.items() if k in df.columns})
            df.columns = [col.upper() for col in df.columns]

            extra_cols = [c for c in df.columns if c not in existing_columns]
            if extra_cols:
                # Create a new column that stores a JSON object of all extra columns
                df[extra_column_name] = df[extra_cols].apply(lambda row: row.to_dict(), axis=1)
                # Replace empty dicts with a dummy key
                df[extra_column_name] = df[extra_column_name].apply(lambda x: x if x else {})
                # Drop extra columns from df (they will be stored in the VARIANT column)
                df = df.drop(columns=extra_cols)

            # --- Step 2: Preprocess DataFrame ---
            # Convert any list columns to OBJECT/ARRAY for Snowflake
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                    df[col] = df[col].apply(lambda x: x if isinstance(x, (list, dict)) else None)

            # --- Step 3: Write to staging table ---
            self.log.info(f"Writing {len(df)} rows to staging table {staging_table}")
            
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                staging_table,
                auto_create_table=True,
                overwrite=True
            )
            if not success:
                raise RuntimeError(f"Failed to write DataFrame to staging table {staging_table}")

            # --- Step 4: Build MERGE statement ---
            join_condition = " AND ".join([f"T.{key.upper()} = S.{key.upper()}" for key in upsert_keys])

            all_cols = df.columns.tolist()
            self.log.debug(f"Columns: {all_cols}")
            
            update_cols = [c for c in all_cols if c.upper() not in [k.upper() for k in upsert_keys]]
            set_clause = ", ".join([f"T.{col} = S.{col}" for col in update_cols])
            insert_cols = ", ".join(all_cols)
            insert_values = ", ".join([f"S.{col}" for col in all_cols])

            merge_sql = f"""
                MERGE INTO {target_table} AS T
                USING {staging_table} AS S
                ON ({join_condition})
                WHEN MATCHED THEN
                    UPDATE SET
                        {set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols})
                    VALUES ({insert_values});
            """

            self.log.info(f"Executing MERGE INTO {target_table}...")
            
            cursor.execute(merge_sql)
            merge_result = cursor.rowcount

            # --- Step 5: Cleanup staging table ---
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

            self.log.info(f"Successfully upserted {merge_result} rows into {target_table}.")
            return merge_result

        finally:
            cursor.close()
            conn.close()


class SnowflakeDB(ConfigurableResource):
    account: str = EnvVar("SNOWFLAKE_ACCOUNT")
    username: str = EnvVar("SNOWFLAKE_USERNAME")
    password: str = EnvVar("SNOWFLAKE_PASSWORD")
    database: str = EnvVar("SNOWFLAKE_DB")
    schema_: str = "PUBLIC"
    warehouse: str = EnvVar("SNOWFLAKE_WAREHOUSE")
    role: str = EnvVar("SNOWFLAKE_ROLE")

    def create_resource(self, context) -> SnowflakeDBClient:
        return SnowflakeDBClient(
            account=self.account,
            username=self.username,
            password=self.password,
            database=self.database,
            schema_=self.schema_,
            warehouse=self.warehouse,
            role=self.role,
            log=context.log
        )
