from config import setup_logging, DB_CONFIG
import logging
from etl.extract import  extract_csv
from etl.load import load_df_to_staging
import pickle
setup_logging()

DATA_DIR = "your/data/folder"
STATE_FILE = "csv_file_state.pkl"
TABLE_MAPPING = {
    "postings": "StgPosting",
    "companies": "StgCompany"
}

def run_pipeline():
    try:

        extracted_csv_data,current_state=extract_csv("data")

        for key, table_name in TABLE_MAPPING.items():
            df = extracted_csv_data.get(key)
            if df is None or df.empty:
                continue
            load_df_to_staging(df, table_name)

       #dave state only after etl is successfully done
        with open(STATE_FILE, 'wb') as f:
            pickle.dump(current_state, f)

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        raise

run_pipeline()


