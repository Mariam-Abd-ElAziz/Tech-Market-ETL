from config import setup_logging, DB_CONFIG
import logging
from etl.extract import  extract_csv
from etl.transform import (
    filter_tech_data, clean_data, standardize_data_format, transform_dim_company,
    transform_dim_location, transform_fact_tech_job, derive_dim_table, transform_bridge_table
)
from etl.load import load_df_to_table
import pickle
from etl.utils import read_table_from_db

setup_logging()
logger = logging.getLogger(__name__)


DATA_DIR = "data"
STATE_FILE = "csv_file_state.pkl"

tables_to_load = {
    "industries": ("dim_industry", "dim"),
    "skills": ("dim_skill", "dim"),
    "postings": ("stg_posting", "staging"),
    "companies": ("stg_company", "staging")
}
def run_pipeline():
    # Extract data
    logger.info("Extracting CSV files...")
    csv_data, new_state = extract_csv(DATA_DIR, STATE_FILE)
    print(csv_data)
    if not csv_data:
        logger.info("No new or modified files found.")
        return

    # Load static dimension tables ( skills, industries) and staging tables (postings,companies)
    for key, (table_name, schema) in tables_to_load.items():
        if key in csv_data:
            df = csv_data[key].dropna()
            logger.info(f"Loading table '{table_name}' into schema '{schema}'")
            load_df_to_table(df, table_name, schema)


    # Get DataFrames
    jobs_df = csv_data.get("postings")
    jobs_industries_df = csv_data.get("job_industries")
    jobs_skills_df = csv_data.get("job_skills")
    companies_df = csv_data.get("companies")
    industries_df = csv_data.get("industries")


    # Filter tech jobs early
    tech_jobs = filter_tech_data(jobs_df, industries_df, jobs_industries_df)
    logger.info(f"Filtered tech jobs: {len(tech_jobs)} rows")

    # Clean & standardize
    boolean_cols = ['remote_allowed']
    required_cols = ['job_id', 'title', 'company_id', 'location']
    object_cols = [ 'formatted_work_type', 'formatted_experience_level', 'location']
    num_cols = ['normalized_salary']

    tech_jobs = clean_data(tech_jobs, boolean_cols, required_cols, object_cols, num_cols)
    tech_jobs = standardize_data_format(tech_jobs, ['original_listed_time'], boolean_cols)

    # DIM COMPANY
    required_cols=['name']
    object_cols=['description','url']

    companies_df=clean_data(companies_df,[],required_cols,object_cols)

    if companies_df is None:
        logger.error("Missing companies.csv.")
        return
    dim_company = transform_dim_company(companies_df)
    load_df_to_table(dim_company, "dim_company","dim")

    # DIM LOCATION
    dim_location = transform_dim_location(tech_jobs)
    load_df_to_table(dim_location, "dim_location","dim")

    # DIM WORK TYPE
    dim_work_type = derive_dim_table(tech_jobs, "formatted_work_type", "work_type_name")
    load_df_to_table(dim_work_type, "dim_work_type","dim")

    # DIM EXP LEVEL
    dim_exp_level = derive_dim_table(tech_jobs, "formatted_experience_level", "experience_level_name")
    load_df_to_table(dim_exp_level, "dim_exp_level","dim")
    #need to extract dim sk
    dim_company=read_table_from_db("dim_company","dim")
    dim_location=read_table_from_db("dim_location","dim")
    dim_work_type=read_table_from_db("dim_work_type","dim")
    dim_exp_level=read_table_from_db("dim_exp_level","dim")
    # FACT TECH JOB
    fact_jobs = transform_fact_tech_job(
        tech_jobs,
        dim_company,
        dim_location,
        dim_work_type,
        dim_exp_level,
    )

    load_df_to_table(fact_jobs, "fact_tech_job","fact")

    fact_jobs=read_table_from_db("fact_tech_job","fact")
    dim_industry=read_table_from_db("dim_industry","dim")
    dim_skill=read_table_from_db("dim_skill","dim")
    # BRIDGE JOB-SKILL
    if 'jobs_skills' in csv_data:
        bridge_skills = transform_bridge_table(
            jobs_skills_df,
            dim_skill,
            fact_jobs,
            bridge_dim_col="skill_abr"
        )
        load_df_to_table(bridge_skills, "bridge_job_skill","bridge")


    # BRIDGE JOB-INDUSTRY
    bridge_industries = transform_bridge_table(
        jobs_industries_df,
        dim_industry,
        fact_jobs,
        bridge_dim_col="industry_id"
    )
    load_df_to_table(bridge_industries, "bridge_job_industry","bridge")

    # Save new state
    with open(STATE_FILE, 'wb') as f:
        pickle.dump(new_state, f)
    logger.info("ETL process completed successfully.")


run_pipeline()