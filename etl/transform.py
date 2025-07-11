import pandas as pd
import pycountry
from sqlalchemy import create_engine, inspect
from sqlalchemy import Integer, Float, String, Boolean, DateTime, Date, Numeric
from dotenv import load_dotenv
import os
load_dotenv()

#Clean#
def clean_data(df: pd.DataFrame,boolean_cols: list[str],required_cols:list[str],
               object_cols:list[str],num_cols:list[str]) -> pd.DataFrame:

    #1-drop duplicates
    df.drop_duplicates(inplace=True)

    #2-fill nulls
    for col in boolean_cols:
        # make nulls false
        df[col] = df[col].fillna(0)

    place_holder="Not Defined"
    for col in object_cols:
        df[col] = df[col].fillna(place_holder)

    for col in num_cols:
        df[col] = df[col].fillna(0)

    #3-drop nulls
    df.dropna(subset=required_cols)

    return df

def standardize_data_format(df: pd.DataFrame, time_cols: list[str],
                            boolean_cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in time_cols:
        df[col] = pd.to_datetime(df[col], unit='ms').dt.date
    for col in boolean_cols:
        #make nulls false
        df[col] = df[col].astype(bool)
    return df


#Filter Data#
def drop_unwanted_cols(df: pd.DataFrame,cols:list[str]) -> pd.DataFrame:
    df = df.copy()
    cols_to_drop = [col for col in cols
                    if col in df.columns]
    df.drop(columns=cols_to_drop)
    return df

def filter_tech_data(jobs_df: pd.DataFrame, industries_df: pd.DataFrame,
                     jobs_industries_df: pd.DataFrame) -> pd.DataFrame:
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
def derive_dim_table(df: pd.DataFrame, col_name: str, table_id: str, table_value: str) -> pd.DataFrame:

    df=df.copy()
    unique_values = df[col_name].dropna().unique()
    new_df=pd.DataFrame(
        {
            table_id :range(0,len(unique_values)),
            table_value :unique_values
        }
    )
    return new_df

def transform_dim_company(df: pd.DataFrame) -> pd.DataFrame:
    wanted_cols = ['company_id', 'name', 'description', 'company_size', 'url']
    df = df[wanted_cols].copy()
    #in case we dropped duplicates from it
    df.reset_index(drop=True, inplace=True)
    df.insert(0, 'company_sk', df.index )
    return df

def is_country(name:str):
    try:
        return pycountry.countries.lookup(name.strip())
    except LookupError:
        return None
def parse_location(location:str):
    if pd.isna(location) or not str(location).strip():
        return pd.Series({'region': None, 'country': None})

    parts = [p.strip() for p in location.split(',') if p.strip()]
    country = None
    region_parts = []

    for part in parts:
        if not country and is_country(part):
            country = is_country(part).name
        else:
            region_parts.append(part)

    region = ', '.join(region_parts) if region_parts else None
    return pd.Series({'region': region, 'country': country})
def transform_dim_location(df: pd.DataFrame, location_col: str = 'location') -> pd.DataFrame:
    df = df.copy()
    parsed = df[location_col].apply(parse_location)
    df_parsed = pd.concat([df[[location_col]], parsed], axis=1)
    location_dim = df_parsed.drop_duplicates().reset_index(drop=True)
    location_dim.insert(0, 'location_id', location_dim.index + 1)

    return location_dim




