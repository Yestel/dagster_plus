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
    RESERVED_COLUMNS: dict = { "by": "_BY", "type": "_TYPE" }

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

    def write_dataframe(self, df: pd.DataFrame, table_name: str, upsert_keys: list):
        """
        Write a Pandas DataFrame to a Snowflake table using upsert (MERGE) logic.

        Args:
            df (pd.DataFrame): The DataFrame to write.
            table_name (str): The name of the target table.
            upsert_keys (list): A list of column names to use as the unique key
                                for determining updates vs. inserts.
        """
        if not upsert_keys:
            raise ValueError("upsert_keys must be provided for upsert logic.")

        # 1. Pre-process the DataFrame
        # Convert any list columns to OBJECT for Snowflake
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: x if isinstance(x, list) else None)

        # Rename reserved columns and convert all column names to uppercase
        df = df.rename(columns={k: v for k, v in self.RESERVED_COLUMNS.items() if k in df.columns})
        df.columns = [col.upper() for col in df.columns]

        target_table = table_name.upper()
        staging_table = f"{target_table}_STG"  # Create a staging table name

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # --- Step 1: Write to Temporary Staging Table ---
            print(f"Writing DataFrame to temporary staging table: {staging_table}")
            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                staging_table,
                auto_create_table=True,  # Automatically create the staging table
                overwrite=True          # Overwrite the staging table if it exists
            )
            if not success:
                raise RuntimeError(f"Failed to write DataFrame to staging table {staging_table}")

            # --- Step 2: Build and Execute the MERGE Statement ---

            # 1. Define the join condition for the MERGE (using the upsert_keys)
            # e.g., "T.KEY1 = S.KEY1 AND T.KEY2 = S.KEY2"
            join_condition = " AND ".join([f"T.{key.upper()} = S.{key.upper()}" for key in upsert_keys])

            # 2. Define the columns to update (all columns *except* the upsert keys)
            all_cols = [c for c in df.columns]
            update_cols = [c for c in all_cols if c.upper() not in [k.upper() for k in upsert_keys]]

            # 3. Define the SET clause for updates
            # e.g., "T.COL1 = S.COL1, T.COL2 = S.COL2"
            set_clause = ", ".join([f"T.{col} = S.{col}" for col in update_cols])

            # 4. Define the VALUES clause for inserts
            # e.g., "(COL1, COL2, ...)" and "(S.COL1, S.COL2, ...)"
            insert_cols = ", ".join(all_cols)
            insert_values = ", ".join([f"S.{col}" for col in all_cols])
            
            # Build the final MERGE statement
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

            print(f"Executing MERGE INTO {target_table}...")
            cursor.execute(merge_sql)
            merge_result = cursor.rowcount

            # --- Step 3: Cleanup the Staging Table ---
            print(f"Cleaning up staging table: {staging_table}")
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

            print(f"Successfully upserted {merge_result} rows into {target_table}.")
            return merge_result

        finally:
            cursor.close()
            conn.close()


