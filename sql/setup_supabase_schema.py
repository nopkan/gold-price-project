from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import unquote, urlparse


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
CONNECTION_ENV_NAME = "AIRFLOW_CONN_SUPABASE_WAREHOUSE"

DDL_FILES = [
    PROJECT_ROOT / "sql" / "bronze" / "01_create_bronze_table.sql",
    PROJECT_ROOT / "sql" / "silver" / "01_create_silver_table.sql",
    PROJECT_ROOT / "sql" / "gold" / "01_create_gold_table.sql",
]


def load_dotenv_if_present() -> None:
    if not ENV_PATH.exists():
        return

    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def get_connection_uri() -> str:
    connection_uri = os.environ.get(CONNECTION_ENV_NAME, "").strip()
    if not connection_uri:
        raise SystemExit(f"Missing {CONNECTION_ENV_NAME} in environment or .env")

    placeholders = ["[PROJECT-REF]", "[YOUR-PASSWORD]", "[REGION]"]
    if any(placeholder in connection_uri for placeholder in placeholders):
        raise SystemExit("Supabase connection still has placeholder values.")

    return connection_uri.replace("postgresql+psycopg2://", "postgresql://", 1)


def connect(connection_uri: str):
    import psycopg2

    parsed = urlparse(connection_uri)
    return psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/") or "postgres",
        user=unquote(parsed.username or ""),
        password=unquote(parsed.password or ""),
        sslmode="require",
        connect_timeout=15,
    )


def main() -> None:
    load_dotenv_if_present()
    connection_uri = get_connection_uri()

    with connect(connection_uri) as conn:
        with conn.cursor() as cursor:
            for ddl_file in DDL_FILES:
                cursor.execute(ddl_file.read_text(encoding="utf-8"))
                print(f"Applied {ddl_file.relative_to(PROJECT_ROOT)}")

            cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema IN ('bronze', 'silver', 'gold')
                ORDER BY table_schema, table_name;
                """
            )
            tables = cursor.fetchall()

    print("Supabase schema setup OK")
    for schema_name, table_name in tables:
        print(f"{schema_name}.{table_name}")


if __name__ == "__main__":
    main()