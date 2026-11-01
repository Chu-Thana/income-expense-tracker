import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


PROJECT_DIR = "/opt/airflow/income_expense_tracker"
ETL_DIR = f"{PROJECT_DIR}/pipeline/etl"



def run_etl(script_name, needs_database=False, script_args=None):
    env = os.environ.copy()

    if needs_database:
        conn = BaseHook.get_connection("income_expense_postgres")

        env.update({
            "DB_HOST": conn.host,
            "DB_PORT": str(conn.port or 5432),
            "POSTGRES_DB": conn.schema,
            "POSTGRES_USER": conn.login,
            "POSTGRES_PASSWORD": conn.password,
        })

    command = [
        sys.executable,
        f"{ETL_DIR}/{script_name}",
    ]

    if script_args:
        command.extend(script_args)

    subprocess.run(
        command,
        env=env,
        cwd=PROJECT_DIR,
        check=True,
    )

with DAG(
    dag_id="income_expense_batch_dag",
    description="Income Expense Batch ETL Prototype",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["income-expense", "prototype"],
) as dag:

    extract = PythonOperator(
        task_id="extract_transactions",
        python_callable=run_etl,
        op_kwargs={
            "script_name": "extract_transactions.py",
            "needs_database": True,
        },
    )

    upload_raw = PythonOperator(
        task_id="upload_raw_to_s3",
        python_callable=run_etl,
        op_kwargs={
            "script_name": "upload_layer_to_s3.py",
            "script_args": ["--layer", "raw"],
        },
    )

    silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_etl,
        op_kwargs={"script_name": "transform_silver.py"},
    )

    upload_silver = PythonOperator(
        task_id="upload_silver_to_s3",
        python_callable=run_etl,
        op_kwargs={
            "script_name": "upload_layer_to_s3.py",
            "script_args": ["--layer", "silver"],
        },
    )

    gold = PythonOperator(
        task_id="build_gold",
        python_callable=run_etl,
        op_kwargs={"script_name": "build_gold.py"},
    )

    validate = PythonOperator(
        task_id="validate_gold",
        python_callable=run_etl,
        op_kwargs={"script_name": "validate_gold.py"},
    )


    upload_s3 = PythonOperator(
        task_id="upload_gold_to_s3",
        python_callable=run_etl,
        op_kwargs={
            "script_name": "upload_gold_to_s3.py",
        },
    )

    load_redshift = PythonOperator(
        task_id="load_gold_to_redshift",
        python_callable=run_etl,
        op_kwargs={
            "script_name": "load_gold_to_redshift.py",
        },
    )

    (
            extract
            >> upload_raw
            >> silver
            >> upload_silver
            >> gold
            >> validate
            >> upload_s3
            >> load_redshift
    )
