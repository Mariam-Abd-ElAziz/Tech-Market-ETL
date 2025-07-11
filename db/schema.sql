CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS fact;
CREATE SCHEMA IF NOT EXISTS bridge;
SET search_path TO staging,dim,fact,bridge;


-- Raw data:
CREATE TABLE IF NOT EXISTS staging.stg_posting(
    job_id TEXT,
    company_name TEXT,
    title TEXT,
    description TEXT,
    max_salary TEXT,
    pay_period TEXT,
    location TEXT,
    company_id TEXT,
    views TEXT,
    med_salary TEXT,
    min_salary TEXT,
    formatted_work_type TEXT,
    applies TEXT,
    original_listed_time TEXT,
    remote_allowed TEXT,
    job_posting_url TEXT,
    application_url TEXT,
    application_type TEXT,
    expiry TEXT,
    closed_time TEXT,
    formatted_experience_level TEXT,
    skills_desc TEXT,
    listed_time TEXT,
    posting_domain TEXT,
    sponsored BOOLEAN,
    work_type TEXT,
    currency TEXT,
    compensation_type TEXT,
    normalized_salary TEXT,
    zip_code TEXT,
    fips TEXT
);

CREATE TABLE IF NOT EXISTS staging.stg_company (
    company_id TEXT,
    name TEXT,
    description TEXT,
    company_size TEXT,
    state TEXT,
    country TEXT,
    city TEXT,
    zip_code TEXT,
    address TEXT,
    url TEXT
);

-- Dimensions:
CREATE TABLE IF NOT EXISTS dim.dim_company(
     company_sk BIGSERIAL PRIMARY KEY,
     company_id BIGINT NOT NULL,
     company_name TEXT NOT NULL,
     company_size  INT,
     description TEXT,
     url TEXT
);

CREATE TABLE IF NOT EXISTS dim.dim_industry (
    industry_id INT PRIMARY KEY,
    industry_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.dim_skill (
    skill_abr VARCHAR(10) PRIMARY KEY,
    skill_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.dim_benefit (
    benefit_id SERIAL PRIMARY KEY,
    benefit_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.dim_work_type (
    work_type_id SERIAL PRIMARY KEY,
    work_type_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.dim_exp_level (
   experience_level_id SERIAL PRIMARY KEY,
   experience_level_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_key INT PRIMARY KEY,
    date DATE NOT NULL,
    day INT NOT NULL,
    day_suffix VARCHAR(2) NOT NULL,
    weekday_name VARCHAR(10) NOT NULL,
    weekday_number INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    dow_in_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    quarter_name VARCHAR(10) NOT NULL,
    year INT NOT NULL,
    iso_year INT NOT NULL,
    first_day_of_month DATE NOT NULL,
    last_day_of_month DATE NOT NULL,
    first_day_of_quarter DATE NOT NULL,
    last_day_of_quarter DATE NOT NULL,
    first_day_of_year DATE NOT NULL,
    last_day_of_year DATE NOT NULL,
    is_leap_year BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim.dim_location (
    location_id SERIAL PRIMARY KEY,
    region TEXT,
    country TEXT,
    location TEXT UNIQUE
);

-- Fact tables:
CREATE TABLE IF NOT EXISTS fact.fact_tech_job(
    job_sk BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL,
    job_title TEXT NOT NULL,
    listing_date_key INT,
    company_id INT,
    location_id INT,
    work_type_id INT,
    experience_level_id INT,
    remote_allowed BOOLEAN,
    salary_exist BOOLEAN,
    normalized_salary FLOAT,

    FOREIGN KEY (listing_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (company_id) REFERENCES dim_company(company_sk),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (work_type_id) REFERENCES dim_work_type(work_type_id),
    FOREIGN KEY (experience_level_id) REFERENCES dim_exp_level(experience_level_id)
);

CREATE TABLE IF NOT EXISTS bridge.bridge_job_skill(
    job_id INT NOT NULL,
    skill_abr VARCHAR(10) NOT NULL,
    PRIMARY KEY (job_id, skill_abr),
    FOREIGN KEY (job_id) REFERENCES fact_tech_job(job_sk),
    FOREIGN KEY (skill_abr) REFERENCES dim_skill(skill_abr)
);

CREATE TABLE IF NOT EXISTS bridge.bridge_job_industry(
    job_id INT NOT NULL,
    industry_id INT NOT NULL,
    PRIMARY KEY (job_id, industry_id),
    FOREIGN KEY (job_id) REFERENCES fact_tech_job(job_sk),
    FOREIGN KEY (industry_id) REFERENCES dim_industry(industry_id)
);

-- Time Dimension Data Load:
INSERT INTO dim.dim_date (
    date_key, date, day, day_suffix,
    weekday_name, weekday_number, is_weekend,
    dow_in_month, day_of_year, week_of_year,
    month, month_name, quarter, quarter_name,
    year, iso_year,
    first_day_of_month, last_day_of_month,
    first_day_of_quarter, last_day_of_quarter,
    first_day_of_year, last_day_of_year,
    is_leap_year
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS date,
    EXTRACT(DAY FROM d)::INT AS day,
    CASE
        WHEN EXTRACT(DAY FROM d)::INT IN (11,12,13) THEN 'th'
        WHEN RIGHT(EXTRACT(DAY FROM d)::TEXT,1) = '1' THEN 'st'
        WHEN RIGHT(EXTRACT(DAY FROM d)::TEXT,1) = '2' THEN 'nd'
        WHEN RIGHT(EXTRACT(DAY FROM d)::TEXT,1) = '3' THEN 'rd'
        ELSE 'th'
    END AS day_suffix,
    TO_CHAR(d, 'FMDay') AS weekday_name,
    EXTRACT(DOW FROM d)::INT + 1 AS weekday_number,
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    ((EXTRACT(DAY FROM d)::INT - 1) / 7 + 1)::INT AS dow_in_month,
    EXTRACT(DOY FROM d)::INT AS day_of_year,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(MONTH FROM d)::INT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    'Q' || EXTRACT(QUARTER FROM d)::INT AS quarter_name,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(ISOYEAR FROM d)::INT AS iso_year,
    DATE_TRUNC('month', d)::DATE AS first_day_of_month,
    (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE AS last_day_of_month,
    DATE_TRUNC('quarter', d)::DATE AS first_day_of_quarter,
    (DATE_TRUNC('quarter', d) + INTERVAL '3 months - 1 day')::DATE AS last_day_of_quarter,
    DATE_TRUNC('year', d)::DATE AS first_day_of_year,
    (DATE_TRUNC('year', d) + INTERVAL '1 year - 1 day')::DATE AS last_day_of_year,
    CASE
        WHEN (EXTRACT(YEAR FROM d)::INT % 4 = 0 AND EXTRACT(YEAR FROM d)::INT % 100 != 0)
             OR (EXTRACT(YEAR FROM d)::INT % 400 = 0)
        THEN TRUE ELSE FALSE
    END AS is_leap_year
FROM generate_series('2000-01-01'::DATE, '2050-12-31'::DATE, INTERVAL '1 day') AS d;