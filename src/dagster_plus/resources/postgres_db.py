import json
import pandas as pd
from dagster import ConfigurableResource, EnvVar
from sqlalchemy import create_engine, text

class PostgresDBClient:
    def __init__(self, username, password, hostname, database, schema_, log):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.database = database
        self.schema_ = schema_
        self.log = log
        self.RESERVED_COLUMNS = { "by": "_by", "type": "_type"}

    @property
    def connection_string(self):
        return f"postgresql://{self.username}:{self.password}@{self.hostname}/{self.database}"

    def get_engine(self):
        self.log.info(f"Creating Postgres engine for {self.hostname}/{self.database}")
        return create_engine(self.connection_string)

    def fetch_dataframe(self, query: str) -> pd.DataFrame:
        engine = self.get_engine()
        self.log.info(f"Executing query: {query}")
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        self.log.info(f"Fetched {len(df)} rows")
        return df
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, upsert_keys: list, extra_column_name: str = "_others"):
        """
        Write DataFrame to Postgres using a staging table for faster upsert.
        - Extra columns are stored in JSONB.
        - Supports arrays and JSONB columns correctly.
        """
        if not upsert_keys:
            raise ValueError("upsert_keys must be provided for upsert logic.")

        engine = self.get_engine()
        schema = self.schema_
        target_table = table_name
        staging_table = f"{target_table}_stg"

        def replace_nan_with_none(d):
            return {k: (None if pd.isna(v) else v) for k, v in d.items()}

        with engine.begin() as conn:

            # -----------------------------
            # Step 0: get target table columns & types
            # -----------------------------
            q = text("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :table;
            """)
            rows = conn.execute(q, {"schema": schema, "table": target_table}).fetchall()
            existing_columns = {r[0].lower() for r in rows}
            column_types = {r[0].lower(): r[1].lower() for r in rows}

            # -----------------------------
            # Step 0: drop staging table
            # -----------------------------
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))

            # -----------------------------
            # Step 1: handle extra columns
            # -----------------------------
            df = df.rename(columns={k: v for k, v in self.RESERVED_COLUMNS.items() if k in df.columns})
            df.columns = [c.lower() for c in df.columns]

            extra_cols = [c for c in df.columns if c not in existing_columns]

            if extra_cols:
                self.log.info(f"Found extra columns: {extra_cols}")
                df[extra_column_name] = df[extra_cols].apply(lambda r: r.to_dict(), axis=1)
                df[extra_column_name] = df[extra_column_name].apply(replace_nan_with_none)
                df = df.drop(columns=extra_cols)
                column_types[extra_column_name] = "jsonb"  # force JSONB

            # -----------------------------
            # Step 2: preprocess list/dict → correct type
            # -----------------------------
            for col in df.columns:
                col_type = column_types.get(col.lower())
                # ARRAY columns: leave list as-is
                if col_type.endswith("[]"):
                    df[col] = df[col].apply(lambda x: x if isinstance(x, list) else None)
                # JSON/JSONB columns
                elif col_type in ("json", "jsonb"):
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                # Scalars: replace list/dict with None
                else:
                    df[col] = df[col].apply(lambda x: None if isinstance(x, (list, dict)) else x)

            # -----------------------------
            # Step 3: write to staging table
            # -----------------------------
            self.log.info(f"Writing {len(df)} rows to staging table {staging_table}")
            # Create staging table with same structure as target
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))
            conn.execute(text(f'CREATE TABLE "{schema}"."{staging_table}" (LIKE "{schema}"."{target_table}" INCLUDING ALL)'))

            df.to_sql(staging_table, conn, schema=schema, if_exists="append", index=False, method='multi')

            # -----------------------------
            # Step 4: UPSERT from staging → main
            # -----------------------------
            all_cols = df.columns.tolist()
            conflict_cols = ", ".join([f'"{k.lower()}"' for k in upsert_keys])
            update_cols = [c for c in all_cols if c.lower() not in [k.lower() for k in upsert_keys]]
            update_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
            insert_cols = ", ".join([f'"{c}"' for c in all_cols])
            insert_values = ", ".join([f'"{c}"' for c in all_cols])

            upsert_sql = f"""
                INSERT INTO "{schema}"."{target_table}" ({insert_cols})
                SELECT {insert_values} FROM "{schema}"."{staging_table}"
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {update_clause};
            """

            self.log.info(f"Upserting {len(df)} rows from staging into {target_table}")
            conn.execute(text(upsert_sql))

            # -----------------------------
            # Step 5: drop staging table
            # -----------------------------
            conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{staging_table}"'))

            self.log.info(f"Upsert complete: {len(df)} rows into {target_table}")
            return len(df)


class PostgresDB(ConfigurableResource):
    hostname: str = EnvVar("POSTGRES_HOSTNAME")
    username: str = EnvVar("POSTGRES_USERNAME")
    password: str = EnvVar("POSTGRES_PASSWORD")
    database: str = EnvVar("POSTGRES_DB")
    schema_: str = "public"

    def create_resource(self, context) -> PostgresDBClient:
        return PostgresDBClient(
            username=self.username,
            password=self.password,
            hostname=self.hostname,
            database=self.database,
            schema_=self.schema_,
            log=context.log
        )
