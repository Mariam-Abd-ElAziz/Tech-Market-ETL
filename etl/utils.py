from sqlalchemy import create_engine
import pandas as pd
import os

def read_table_from_db(table_name: str, schema_name: str = "public") -> pd.DataFrame:
    db_url = os.getenv("DB_URL")
    engine = create_engine(db_url)
    df = pd.read_sql(f"SELECT * FROM {schema_name}.{table_name}", engine)
    return df