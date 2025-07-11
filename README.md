
# 🛠️ Tech Jobs ETL Pipeline 

## 📊 Overview

This project implements a real-world **ETL pipeline** to simulate how raw job posting data is extracted, transformed, and loaded into a PostgreSQL database for structured analysis and visualization. 
---

## 📁 Dataset Source & Structure

- **Source**: [LinkedIn Job Postings Dataset](https://www.kaggle.com/datasets/arshkon/linkedin-job-postings) from Kaggle.
- **Description**: The dataset includes job listings posted on LinkedIn, including job details, company information, required skills, and industry classification.
- **Files**:
  - `postings.csv` - Raw job postings
  - `companies.csv` - Company metadata
  - `skills.csv` - Skill definitions
  - `industries.csv` - Industry definitions
  - `job_industries.csv`, `job_skills.csv` - Bridge tables for many-to-many relationships

> The ETL process automatically detects new or modified files using incremntal loading logic by using file timestamps to avoid redundant processing.


---

## 🔄 ETL Steps and Logic

### 1. **Extraction** (`extract_csv`)
- Scans a given data directory for `.csv` files.
- Detects changes using file timestamps and loads only new or modified files.
- Uses `pandas.read_csv` for data ingestion.

### 2. **Transformation**
Modular functions under `etl/transform.py` handle:
- **Cleaning** (`clean_data`) — Null handling, duplicates, type consistency.
- **Standardization** — Dates (ms to `datetime.date`), booleans.
- **Filtering** — Extract only **tech jobs** using regex on industry names.
- **Dimension Creation**:
  - Companies (`transform_dim_company`)
  - Locations (`transform_dim_location`)
  - Work Types, Experience Levels (`derive_dim_table`)
- **Fact Table**:
  - `fact_tech_job` aggregates cleaned job data with keys to all related dimensions.
- **Bridge Tables**:
  - `bridge_job_skill`, `bridge_job_industry` link jobs to skills and industries.

### 3. **Loading**
- Loads data using PostgreSQL `COPY` command via `psycopg2`.
- Loads staging, dimension, fact, and bridge tables across these schemas:
  - `staging`, `dim`, `fact`, `bridge`

---

## 🧱 Database Schema Design

The project uses a **star schema** layout for analytics-friendly design.

### ✅ Schemas & Tables
- `dim.dim_company`, `dim.dim_skill`, `dim.dim_industry`, `dim.dim_location`, `dim.dim_exp_level`, `dim.dim_work_type`
- `fact.fact_tech_job`
- `bridge.bridge_job_skill`, `bridge.bridge_job_industry`
- `staging.stg_posting`, `staging.stg_company`

### ⏱️ Time Dimension
- A comprehensive `dim_date` table is pre-populated from 2000–2050 using PostgreSQL SQL.

---

## 📈 Visualization

Visualizations were created using **Power BI** to provide meaningful insights from the processed tech job data. The reports include:

- Avg Salary of tech jobs
- Top hiring companies in the tech sector
- Job counts across countries, different experince levels, differnt work types

> 📂 Power BI report files and screenshots are available in the `/power_bi/` folder.

## 🧪 How to Run the Project Locally

### ⚙️ Requirements
- Python 3.9+
- PostgreSQL
- Libraries: `pandas`, `psycopg2`, `sqlalchemy`, `python-dotenv`, `pycountry`, etc.

### 📦 Installation
```bash
pip install -r requirements.txt
```

### 🛠️ Setup

1. Create a `.env` file in the root directory with the following content:
```
DB_NAME=your_DB_Name
DB_USER=your_DB_USER
DB_PASSWORD=your_DB_PASSWORD
DB_HOST=localhost
DB_PORT=5432

DB_URL=postgresql+psycopg2://your_DB_USER:your_DB_PASSWORD@localhost:5432/your_DB_Name
```


2. Set up database schemas and tables:
```bash
psql -U postgres -d tech_market -f schema.sql
```

3. Place CSV files into the `/data/` directory.

4. Run the ETL pipeline:
```bash
python run_pipeline.py
```

> Logs will be saved to `tech-market-etl.log` and displayed in the console.

---