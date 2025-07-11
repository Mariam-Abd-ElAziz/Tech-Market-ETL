from io import StringIO
import psycopg2
import pandas as pd
from config import DB_CONFIG
import logging
logger = logging.getLogger(_name_)

def load_df_to_table(df:pd.DataFrame, table_name:str,schema_name:str="public"):
    try:
        tmp = StringIO()
        df.to_csv(tmp, index=False, header=True)
        tmp.seek(0)
        columns = ', '.join(df.columns)

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                    copy_sql = f"""
                            COPY {schema_name}.{table_name}  ({columns})
                            FROM STDIN WITH CSV HEADER
                        """
                    cursor.copy_expert(copy_sql, tmp)

    except Exception as e:
        logging.error(f"Failed to load Data into {schema_name}.{table_name}:{e}")