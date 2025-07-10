CREATE SCHEMA IF NOT EXISTS Staging;
CREATE SCHEMA IF NOT EXISTS Dim;
CREATE SCHEMA IF NOT EXISTS Fact;
SET search_path TO Staging,Dim,Fact;

-- Raw data:
CREATE TABLE IF NOT EXISTS Staging.StgPosting(
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

CREATE TABLE IF NOT EXISTS Staging.StgCompany (
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

--Dimensions:
CREATE TABLE IF NOT EXISTS Dim.DimCompany(
     company_sk BIGSERIAL PRIMARY KEY,
     company_id BIGINT NOT NULL,
     company_name TEXT NOT NULL,
     company_size  INT,
     description TEXT,
     url TEXT
);
CREATE TABLE IF NOT EXISTS Dim.DimIndustry (
    industry_id INT PRIMARY KEY,
    industry_name TEXT UNIQUE Not NULL
);

CREATE TABLE IF NOT EXISTS Dim.DimSkill (
    skill_id VARCHAR(10) PRIMARY KEY,
    skill_name TEXT UNIQUE Not NULL
);

CREATE TABLE IF NOT EXISTS Dim.DimBenefit (
    benefit_id SERIAL PRIMARY KEY,
    benefit_name TEXT UNIQUE Not NULL
);

CREATE TABLE IF NOT EXISTS Dim.DimWorkType (
    work_type_id SERIAL PRIMARY KEY,
    work_type_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS Dim.DimExpLevel (
   experience_level_id SERIAL PRIMARY KEY,
   experience_levelname TEXT UNIQUE
);


CREATE TABLE IF NOT EXISTS Dim.DimDate (
    DateKey INT PRIMARY KEY,
    Date DATE NOT NULL,
    Day INT NOT NULL,
    DaySuffix VARCHAR(2) NOT NULL,
    WeekdayName VARCHAR(10) NOT NULL,
    WeekdayNumber INT NOT NULL,
    IsWeekend BOOLEAN NOT NULL,
    DOW_IN_MONTH INT NOT NULL,
    DayOfYear INT NOT NULL,
    WeekOfYear INT NOT NULL,
    Month INT NOT NULL,
    MonthName VARCHAR(10) NOT NULL,
    Quarter INT NOT NULL,
    QuarterName VARCHAR(10) NOT NULL,
    Year INT NOT NULL,
    ISOYear INT NOT NULL,
    FirstDayOfMonth DATE NOT NULL,
    LastDayOfMonth DATE NOT NULL,
    FirstDayOfQuarter DATE NOT NULL,
    LastDayOfQuarter DATE NOT NULL,
    FirstDayOfYear DATE NOT NULL,
    LastDayOfYear DATE NOT NULL,
    IsLeapYear BOOLEAN NOT NULL,
    IsHoliday BOOLEAN DEFAULT FALSE
);


CREATE TABLE If  NOT EXISTS Dim.DimLocation (
    location_id SERIAL PRIMARY KEY,
    city TEXT,
    state TEXT,
    country TEXT,
    location TEXT UNIQUE --for unstructured locations
);


--Fact tables:
CREATE TABLE If  NOT EXISTS Fact.FactTechJob(
    job_sk BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL,
    job_title TEXT NOT NULL,
    listing_date_key  INT,
    company_id INT,
    location_id INT,
    work_type_id INT,
    experience_level_id INT,
    benefit_id INT,
    remote_allowed BOOLEAN ,
    normalized_salary FLOAT,

    FOREIGN KEY (listing_date_key) REFERENCES DimDate(DateKey),
    FOREIGN KEY (company_id) REFERENCES DimCompany(company_sk),
    FOREIGN KEY (location_id) REFERENCES DimLocation(location_id),
    FOREIGN KEY (work_type_id) REFERENCES DimWorkType(work_type_id),
    FOREIGN KEY (experience_level_id) REFERENCES DimExpLevel(experience_level_id),
    FOREIGN KEY (benefit_id) REFERENCES DimBenefit(benefit_id)
);
CREATE TABLE IF NOT EXISTS Fact.BridgeJobSkill(
    job_id     INT NOT NULL,
    skill_id   VARCHAR(10) NOT NULL,
    PRIMARY KEY (job_id, skill_id),
    FOREIGN KEY (job_id) REFERENCES FactTechJob(job_sk),
    FOREIGN KEY (skill_id) REFERENCES DimSkill(skill_id)
);

CREATE TABLE IF NOT EXISTS Fact.BridgeJobIndustry(
    job_id     INT NOT NULL,
    industry_id   INT NOT NULL,
    PRIMARY KEY (job_id, industry_id),
    FOREIGN KEY (job_id) REFERENCES FactTechJob(job_sk),
    FOREIGN KEY (industry_id) REFERENCES DimIndustry(industry_id)
);

--Time Dim Script
INSERT INTO Dim.DimDate (
    DateKey, Date, Day, DaySuffix,
    WeekdayName, WeekdayNumber, IsWeekend,
    DOW_IN_MONTH, DayOfYear, WeekOfYear,
    Month, MonthName, Quarter, QuarterName,
    Year, ISOYear,
    FirstDayOfMonth, LastDayOfMonth,
    FirstDayOfQuarter, LastDayOfQuarter,
    FirstDayOfYear, LastDayOfYear,
    IsLeapYear
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS DateKey,
    d AS Date,
    EXTRACT(DAY FROM d)::INT AS Day,
    CASE
        WHEN EXTRACT(DAY FROM d)::INT IN (11,12,13) THEN 'th'
        WHEN RIGHT(EXTRACT(DAY FROM d)::TEXT,1) = '1' THEN 'st'
        WHEN RIGHT(EXTRACT(DAY FROM d)::TEXT,1) = '2' THEN 'nd'
        WHEN RIGHT(EXTRACT(DAY FROM d)::TEXT,1) = '3' THEN 'rd'
        ELSE 'th'
    END AS DaySuffix,
    TO_CHAR(d, 'FMDay') AS WeekdayName,
    EXTRACT(DOW FROM d)::INT + 1 AS WeekdayNumber, -- Sunday=1 to Saturday=7
    CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS IsWeekend,
    ((EXTRACT(DAY FROM d)::INT - 1) / 7 + 1)::INT AS DOW_IN_MONTH,
    EXTRACT(DOY FROM d)::INT AS DayOfYear,
    EXTRACT(WEEK FROM d)::INT AS WeekOfYear,
    EXTRACT(MONTH FROM d)::INT AS Month,
    TO_CHAR(d, 'Month') AS MonthName,
    EXTRACT(QUARTER FROM d)::INT AS Quarter,
    'Q' || EXTRACT(QUARTER FROM d)::INT AS QuarterName,
    EXTRACT(YEAR FROM d)::INT AS Year,
    EXTRACT(ISOYEAR FROM d)::INT AS ISOYear,
    DATE_TRUNC('month', d)::DATE AS FirstDayOfMonth,
    (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE AS LastDayOfMonth,
    DATE_TRUNC('quarter', d)::DATE AS FirstDayOfQuarter,
    (DATE_TRUNC('quarter', d) + INTERVAL '3 months - 1 day')::DATE AS LastDayOfQuarter,
    DATE_TRUNC('year', d)::DATE AS FirstDayOfYear,
    (DATE_TRUNC('year', d) + INTERVAL '1 year - 1 day')::DATE AS LastDayOfYear,
    CASE
        WHEN (EXTRACT(YEAR FROM d)::INT % 4 = 0 AND EXTRACT(YEAR FROM d)::INT % 100 != 0)
             OR (EXTRACT(YEAR FROM d)::INT % 400 = 0)
        THEN TRUE ELSE FALSE
    END AS IsLeapYear
FROM generate_series('2000-01-01'::DATE, '2050-12-31'::DATE, INTERVAL '1 day') AS d;
