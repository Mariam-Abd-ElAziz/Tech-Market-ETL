import pandas as pd
import pycountry
from dotenv import load_dotenv
import os

from sqlalchemy.dialects.mssql.information_schema import columns

load_dotenv()

#Clean#
def clean_data(df: pd.DataFrame,
    boolean_cols: list[str] = [],
    required_cols: list[str] = [],
    object_cols: list[str] = [],
    num_cols: list[str] = []) -> pd.DataFrame:
    df=df.copy()
    """
    Cleans the dataframe :
       - Drop duplicates
       - Fill missing boolean, text, and numeric fields
       - Drop rows that are missing required fields
    """
    #1-drop duplicates
    df.drop_duplicates(inplace=True)

    #2-fill nulls
    for col in boolean_cols:
        # make nulls false
        df[col] = df[col].fillna(0)

    place_holder="Not Defined"
    for col in object_cols:
        df[col] = df[col].replace(r'^\s*$', pd.NA, regex=True).fillna(place_holder)

    for col in num_cols:
        df[col] = df[col].fillna(0)

    #3-drop nulls
    df.dropna(subset=required_cols,inplace=True)

    return df

def standardize_data_format(df: pd.DataFrame, time_cols: list[str]=[],
                            boolean_cols: list[str]=[]) -> pd.DataFrame:
    """ Standardizes the format for datetime and boolean fields."""
    df = df.copy()
    for col in time_cols:
        df[col] = pd.to_datetime(df[col], unit='ms').dt.date
    for col in boolean_cols:
        df[col] = df[col].astype(bool)
    return df

#Filter Data#

def filter_tech_data(jobs_df: pd.DataFrame, industries_df: pd.DataFrame,
                     jobs_industries_df: pd.DataFrame) -> pd.DataFrame:
    """ Filters job postings to include only those related to tech industries"""
    tech_keywords = [
        r'\bsoftware\b',
        r'\bdata\b',
        r'\binformation\b',
        r'\bIT\b',
        r'\btechnology\b'
    ]
    pattern = '|'.join(tech_keywords)

    tech_industries_ids = industries_df[
        industries_df['industry_name'].str.contains(pattern, case=False, na=False)]['industry_id']

    tech_job_ids = jobs_industries_df[
        jobs_industries_df['industry_id'].isin(tech_industries_ids)]['job_id'].unique()

    tech_jobs = jobs_df[jobs_df['job_id'].isin(tech_job_ids)]


    return tech_jobs


#Transforms#
def derive_dim_table(df: pd.DataFrame, col_name: str, table_value: str) -> pd.DataFrame:
    """Creates a dimension table from unique values in a column."""
    df=df.copy()
    unique_values = df[col_name].dropna().unique()
    new_df=pd.DataFrame(
        {
            table_value :unique_values
        }
    )
    return new_df

def transform_dim_company(df: pd.DataFrame) -> pd.DataFrame:
    wanted_cols = ['company_id', 'name' ,'company_size', 'description','url']
    df = df[wanted_cols].copy()
    df.rename(columns={"name": "company_name"}, inplace=True)
    df["company_size"] = df["company_size"].astype("Int64")
    #in case we dropped duplicates from it
    df.reset_index(drop=True, inplace=True)
    return df

def is_country(name:str):
    try:
        return pycountry.countries.lookup(name.strip())
    except LookupError:
        return None

def parse_location(location:str):
    """
    Splits a location string into 'region' and 'country'.
    Identifies country using pycountry.
    """
    if pd.isna(location) or not str(location).strip():
        return pd.Series({'region': None, 'country': None})

    parts = [p.strip() for p in location.split(',') if p.strip()]
    country = None
    region_parts = []

    for part in parts:
        country_flag = is_country(part)
        if not country and country_flag:
            country = country_flag.name
        else:
            region_parts.append(part)

    region = ', '.join(region_parts) if region_parts else None
    return pd.Series({'region': region, 'country': country})

def transform_dim_location(df: pd.DataFrame, location_col: str = 'location') -> pd.DataFrame:
    """Transforms raw location strings into structured region + country dimension to match dimension table."""
    df = df.copy()
    parsed = df[location_col].apply(parse_location)
    df_parsed = pd.concat([df[[location_col]], parsed], axis=1)
    location_dim = df_parsed.drop_duplicates().reset_index(drop=True)
    return location_dim

def transform_fact_tech_job(
    tech_jobs: pd.DataFrame,
    dim_company: pd.DataFrame,
    dim_location: pd.DataFrame,
    dim_work_type: pd.DataFrame,
    dim_exp_level: pd.DataFrame,
) -> pd.DataFrame:
    """Builds a fact table of tech jobs with foreign keys to related dimension tables."""
    df = tech_jobs.copy()
    df['original_listed_time'] = pd.to_datetime(df['original_listed_time'])
    #derive date key
    df['listing_date_key'] = df['original_listed_time'].dt.strftime('%Y%m%d').astype(int)

    df = (
        df
        .merge(dim_company[['company_id', 'company_sk']], on='company_id', how='left')
        .merge(dim_location[['location', 'location_id']], on='location', how='left')
        .merge(dim_work_type[['work_type_name', 'work_type_id']], left_on ="formatted_work_type",right_on="work_type_name", how='left')
        .merge(dim_exp_level[['experience_level_name', 'experience_level_id']], left_on="formatted_experience_level",right_on='experience_level_name', how='left')
    )
    df['salary_exist'] = df['normalized_salary'].notnull() & (df['normalized_salary'] > 0)
    fact_df  = df[[
        'job_id',
        'title',
        'listing_date_key',
        'company_sk',
        'location_id',
        'work_type_id',
        'experience_level_id',
        'remote_allowed',
        'salary_exist',
        'normalized_salary'
    ]].rename(columns={'company_sk': 'company_id','title':'job_title'})

    return fact_df

def transform_bridge_table( bridge_df: pd.DataFrame, dim_df: pd.DataFrame,
    fact_df: pd.DataFrame,
    bridge_dim_col: str ,
    join_key: str = "job_id",
    fact_surrogate_col: str = "job_sk",
    output_fact_col: str = "job_id") -> pd.DataFrame:
    """
       Builds a bridge (many-to-many) table linking fact and dimension tables.
       Example: jobs to industries.
    """
    df = bridge_df.copy()
    df = df.merge(
        fact_df[[join_key, fact_surrogate_col]],
        on=join_key,
        how="inner"
    )
    if dim_df is not None:
        valid_keys = set(dim_df[bridge_dim_col].unique())
        df = df[df[bridge_dim_col].isin(valid_keys)]

    df = df[[fact_surrogate_col, bridge_dim_col]].drop_duplicates()
    df = df.rename(columns={fact_surrogate_col: output_fact_col})
    return df