from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task
from psycopg2.extras import Json

WAREHOUSE_CONN_ID = "supabase_warehouse"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = Path(os.environ.get("GOLD_SQL_DIR", PROJECT_ROOT / "sql"))


def read_sql(relative_path: str) -> str:
    sql_path = SQL_DIR / relative_path
    with open(sql_path, "r", encoding="utf-8") as sql_file:
        return sql_file.read()


def build_api_request() -> tuple[str, dict[str, str], dict[str, str], str, str]:
    api_url = os.environ.get("GOLD_API_URL", "https://www.goldapi.io/api/XAU/USD")
    api_key = os.environ.get("GOLD_API_KEY", "")
    symbol = os.environ.get("GOLD_API_SYMBOL", "XAU")
    currency = os.environ.get("GOLD_API_CURRENCY", "USD")

    if not api_key or api_key == "replace_me":
        raise AirflowException("Set GOLD_API_KEY in .env before running the DAG")

    headers = {
        "Accept": "application/json",
        "User-Agent": "gold-price-airflow-pipeline/1.0",
        "x-access-token": api_key,
    }

    return api_url, {}, headers, symbol, currency


@dag(
    dag_id="hourly_gold_price_pipeline",
    description="Hourly gold price API ingestion using Bronze, Silver, and Gold warehouse layers.",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["gold-price", "api", "bronze-silver-gold"],
)
def hourly_gold_price_pipeline() -> None:
    create_bronze = SQLExecuteQueryOperator(
        task_id="create_bronze_table",
        conn_id=WAREHOUSE_CONN_ID,
        sql=read_sql("bronze/01_create_bronze_table.sql"),
    )

    create_silver = SQLExecuteQueryOperator(
        task_id="create_silver_table",
        conn_id=WAREHOUSE_CONN_ID,
        sql=read_sql("silver/01_create_silver_table.sql"),
    )

    create_gold = SQLExecuteQueryOperator(
        task_id="create_gold_table",
        conn_id=WAREHOUSE_CONN_ID,
        sql=read_sql("gold/01_create_gold_table.sql"),
    )

    @task(task_id="extract_api_to_bronze")
    def extract_api_to_bronze() -> int:
        context = get_current_context()
        request_ts = (
            context.get("logical_date")
            or context.get("data_interval_start")
            or context.get("run_after")
            or pendulum.now("UTC")
        )
        api_url, params, headers, symbol, currency = build_api_request()

        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        try:
            payload = response.json()
        except ValueError as exc:
            raise AirflowException("Gold price API did not return valid JSON") from exc

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO bronze.gold_price_raw (
                        request_ts,
                        symbol,
                        currency,
                        source,
                        api_status,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (
                        request_ts,
                        symbol,
                        currency,
                        api_url,
                        response.status_code,
                        Json(payload),
                    ),
                )
                inserted_id = cursor.fetchone()[0]
            conn.commit()

        return inserted_id

    upsert_silver = SQLExecuteQueryOperator(
        task_id="upsert_silver_from_bronze",
        conn_id=WAREHOUSE_CONN_ID,
        sql=read_sql("silver/02_upsert_silver_from_bronze.sql"),
    )

    refresh_gold = SQLExecuteQueryOperator(
        task_id="refresh_gold_hourly_mart",
        conn_id=WAREHOUSE_CONN_ID,
        sql=read_sql("gold/02_refresh_gold_hourly_mart.sql"),
    )

    [create_bronze, create_silver, create_gold] >> extract_api_to_bronze() >> upsert_silver >> refresh_gold


hourly_gold_price_pipeline()
