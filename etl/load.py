from io import StringIO
import psycopg2
import pandas as pd
from config import DB_CONFIG
import logging
logger = logging.getLogger(__name__)

def load_df_to_staging(df:pd.DataFrame, table_name:str):
    try:
        tmp = StringIO()
        df.to_csv(tmp, index=False, header=True)
        tmp.seek(0)
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                    copy_sql = f"""
                            COPY Staging.{table_name}
                            FROM STDIN WITH CSV HEADER
                        """
                    cursor.copy_expert(copy_sql, tmp)

    except Exception as e:
        logging.error(f"Failed to load CSV into staging.{table_name}: {e}")



