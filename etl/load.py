import psycopg2
import pandas as pd
from config import DB_CONFIG
import logging
logger = logging.getLogger(__name__)

def load_csv_to_staging(csv_path, table_name):
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                with open(csv_path, 'r') as f:
                    copy_sql = f"""
                            COPY Staging.{table_name}
                            FROM STDIN WITH CSV HEADER
                        """
                    cursor.copy_expert(copy_sql, f)

    except Exception as e:
        logging.error(f"Failed to load CSV into staging.{table_name}: {e}")

