import logging
import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "shibin",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DATA_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
DB_PATH  = Variable.get("covid_db_path", default_var="/opt/airflow/data/covid_warehouse.db")

COUNTRIES = [
    # picked these 9 bc they have the most complete owid data`n    "United States", "India", "Brazil", "United Kingdom",
    "France", "Germany", "Italy", "Canada", "Australia"
]


# learned this pattern from the weather pipeline - plain /tmp/covid_raw.parquet breaks concurrent runs`ndef _tmp(run_id: str, name: str) -> str:
    """Run-scoped temp path so concurrent DAG runs don't clobber each other."""
    safe = run_id.replace(":", "_").replace("+", "_")
    return f"/tmp/covid_{safe}_{name}.parquet"

def extract_covid_data(**ctx):
    raw_path = _tmp(ctx["run_id"], "raw")
    df = pd.read_csv(DATA_URL)
    log.info("Extracted %d rows across %d countries", len(df), df["location"].nunique())
    df.to_parquet(raw_path, index=False)
    ctx["ti"].xcom_push(key="raw_path", value=raw_path)


def transform_clean(**ctx):
    raw_path = ctx["ti"].xcom_pull(key="raw_path")
    df = pd.read_parquet(raw_path)

    # tried all countries first but a lot had huge data gaps, keeping the 9 most complete
    # filter to the 9 target countries only
    df = df[df["location"].isin(COUNTRIES)].copy()
    log.info("After country filter: %d rows", len(df))

    # keep only the columns we actually need
    cols = [
        "location", "date", "total_cases", "total_deaths",
        "new_cases", "new_deaths", "population",
        "total_vaccinations", "continent",
    ]
    df = df[cols]

    df["date"] = pd.to_datetime(df["date"])
    df["total_cases"]  = df["total_cases"].fillna(0)
    df["total_deaths"] = df["total_deaths"].fillna(0)
    df["new_cases"]    = df["new_cases"].fillna(0)
    df["new_deaths"]   = df["new_deaths"].fillna(0)

    # death_rate: only compute where cases > 0 to avoid divide-by-zero
    df["death_rate_pct"] = df.apply(
        lambda r: round(r["total_deaths"] / r["total_cases"] * 100, 4)
        if r["total_cases"] > 0 else 0.0,
        axis=1,
    )

    clean_path = _tmp(ctx["run_id"], "clean")
    df.to_parquet(clean_path, index=False)
    ctx["ti"].xcom_push(key="clean_path", value=clean_path)
    log.info("Transform complete: %d rows, %d cols", *df.shape)


def spark_analysis(**ctx):
    from pyspark.sql import SparkSession
    clean_path = ctx["ti"].xcom_pull(key="clean_path")

    spark = (
        SparkSession.builder
        .appName("COVID19_Analysis")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    df = pd.read_parquet(clean_path)
    sdf = spark.createDataFrame(df)
    sdf.createOrReplaceTempView("covid")

    log.info("=== Total cases by country ===")
    spark.sql("""
        SELECT location,
               MAX(total_cases) as total_cases,
               MAX(total_deaths) as total_deaths,
               ROUND(MAX(death_rate_pct), 2) as peak_death_rate_pct
        FROM covid
        GROUP BY location
        ORDER BY total_cases DESC
    """).show(truncate=False)

    log.info("=== Monthly US trend ===")
    spark.sql("""
        SELECT YEAR(date) as yr, MONTH(date) as mo,
               SUM(new_cases) as monthly_cases
        FROM covid
        WHERE location = 'United States'
        GROUP BY yr, mo
        ORDER BY yr, mo
        LIMIT 12
    """).show()

    spark.stop()


def load_duckdb(**ctx):
    clean_path = ctx["ti"].xcom_pull(key="clean_path")
    df = pd.read_parquet(clean_path)

    conn = duckdb.connect(DB_PATH)
    # full refresh for now, could make this incremental later`n    conn.execute("DROP TABLE IF EXISTS covid_data")
    conn.execute("CREATE TABLE covid_data AS SELECT * FROM df")
    n = conn.execute("SELECT COUNT(*) FROM covid_data").fetchone()[0]
    log.info("covid_data loaded: %d rows", n)
    conn.close()


def data_quality_checks(**ctx):
    conn = duckdb.connect(DB_PATH)
    failures = []

    def check(label, sql, expected=0):
        val = conn.execute(sql).fetchone()[0]
        status = "PASS" if val == expected else "FAIL"
        log.info("[%s] %s: %s", status, label, val)
        if val != expected:
            failures.append(f"{label}: got {val}, expected {expected}")

    # owid has some aggregate rows like 'World' and 'Europe' that slip through if you're not careful`n    check("No null locations",    "SELECT COUNT(*) FROM covid_data WHERE location IS NULL")
    check("No null dates",        "SELECT COUNT(*) FROM covid_data WHERE date IS NULL")
    check("No negative cases",    "SELECT COUNT(*) FROM covid_data WHERE new_cases < 0")
    check("9 countries present",  "SELECT COUNT(DISTINCT location) FROM covid_data", expected=9)
    check("No death rate > 100",  "SELECT COUNT(*) FROM covid_data WHERE death_rate_pct > 100")

    conn.close()
    if failures:
        raise ValueError(f"Quality checks failed: {failures}")
    log.info("All quality checks passed.")


with DAG(
    dag_id="covid19_pipeline",
    description="Daily ETL for COVID-19 data from Our World in Data",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["covid", "etl", "duckdb", "pyspark"],
) as dag:

    t1 = PythonOperator(task_id="extract_covid_data",  python_callable=extract_covid_data)
    t2 = PythonOperator(task_id="transform_clean",     python_callable=transform_clean)
    t3 = PythonOperator(task_id="spark_analysis",      python_callable=spark_analysis)
    t4 = PythonOperator(task_id="load_duckdb",         python_callable=load_duckdb)
    t5 = PythonOperator(task_id="data_quality_checks", python_callable=data_quality_checks)

    t1 >> t2 >> t3 >> t4 >> t5










