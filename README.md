# Gold Price Airflow Pipeline

Hourly Airflow pipeline that ingests gold price data from GoldAPI into Supabase/Postgres and maintains Bronze, Silver, and Gold warehouse tables.

## Project Layout

- `dags/hourly_gold_price_pipeline.py` - Airflow DAG for API extraction and warehouse transformations.
- `sql/bronze/` - Raw API payload storage.
- `sql/silver/` - Cleaned price observations.
- `sql/gold/` - Hourly reporting mart.
- `sql/setup_supabase_schema.py` - Optional schema bootstrap script.
- `config/simple_auth_manager_passwords.json` - Airflow UI username/password config for this assignment.
- `Containerfile` - Airflow Docker image definition.
- `compose.yaml` - Local Docker Compose service for Airflow standalone.

## Environment

This assignment bundle includes a `.env` file. Docker Compose reads that file and injects the variables into the Airflow container at runtime.

The `.env` file should contain these values:

```env
GOLD_API_URL=https://www.goldapi.io/api/XAU/USD
GOLD_API_KEY=your_goldapi_key
GOLD_API_SYMBOL=XAU
GOLD_API_CURRENCY=USD
AIRFLOW_CONN_SUPABASE_WAREHOUSE=postgresql+psycopg2://user:password@host:5432/database?sslmode=require
```

If your Supabase password has special characters, URL-encode it before putting it in the connection URI.

## Run With Docker

Install and open Docker Desktop first. Confirm the CLI is available:

```zsh
docker --version
docker compose version
```

The examples use the modern Docker Compose command. If your machine uses the legacy binary, replace `docker compose` with `docker-compose`.

Build and start Airflow in the background:

```zsh
docker compose -f compose.yaml up --build -d
```

The Airflow UI runs at:

```text
http://localhost:8080
```

Login:

```text
Username: admin
Password: admin
```

To change the password, edit `config/simple_auth_manager_passwords.json`, then rebuild and recreate the container:

```zsh
docker compose -f compose.yaml up --build -d --force-recreate
```

Stop Airflow:

```zsh
docker compose -f compose.yaml down
```

## Useful Commands

List DAGs:

```zsh
docker compose -f compose.yaml exec airflow airflow dags list
```

Trigger the pipeline:

```zsh
docker compose -f compose.yaml exec airflow airflow dags trigger hourly_gold_price_pipeline
```

List DAG runs:

```zsh
docker compose -f compose.yaml exec airflow airflow dags list-runs hourly_gold_price_pipeline -o plain
```

Run the schema setup script:

```zsh
docker compose -f compose.yaml exec airflow python /opt/airflow/sql/setup_supabase_schema.py
```

Follow logs:

```zsh
docker compose -f compose.yaml logs -f airflow
```

## Local Python

When using Docker, you do not need `.airflow_venv` to run Airflow. The runtime Python environment lives inside the Docker image. A local virtual environment is only useful for VS Code autocomplete or running quick local scripts.
