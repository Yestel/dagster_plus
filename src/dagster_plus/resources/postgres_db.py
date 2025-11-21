from dagster import ConfigurableResource, EnvVar
from sqlalchemy import create_engine
import pandas as pd

class PostgresDB(ConfigurableResource):
    hostname: str = EnvVar("POSTGRES_HOSTNAME")
    username: str = EnvVar("POSTGRES_USERNAME")
    password: str = EnvVar("POSTGRES_PASSWORD")
    database: str = EnvVar("POSTGRES_DB")
    schema_: str = "public"

    @property
    def connection_string(self):
        return f"postgresql://{self.username}:{self.password}@{self.hostname}/{self.database}"

    def get_engine(self):
        return create_engine(self.connection_string)
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append"):
        engine = self.get_engine()
        with engine.connect() as conn:
            df.to_sql(table_name, conn, schema=self.schema_, if_exists=if_exists, index=False)
