from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator      # Airflow 3 import

DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_PROJECT = "/opt/airflow/dbt/bitego"

COPY_RAW = [
    "USE WAREHOUSE BITEGO_WH",
    "COPY INTO BITEGO.RAW.restaurants FROM @BITEGO.RAW.BITEGO_RAW_STAGE/restaurants/  ON_ERROR='CONTINUE'",
    "COPY INTO BITEGO.RAW.users       FROM @BITEGO.RAW.BITEGO_RAW_STAGE/users/        ON_ERROR='CONTINUE'",
    "COPY INTO BITEGO.RAW.food        FROM @BITEGO.RAW.BITEGO_RAW_STAGE/food/         ON_ERROR='CONTINUE'",
    "COPY INTO BITEGO.RAW.menu        FROM @BITEGO.RAW.BITEGO_RAW_STAGE/menu/         ON_ERROR='CONTINUE'",
    "COPY INTO BITEGO.RAW.orders      FROM @BITEGO.RAW.BITEGO_RAW_STAGE/orders/",
    "COPY INTO BITEGO.RAW.order_items FROM @BITEGO.RAW.BITEGO_RAW_STAGE/order_items/",
    "COPY INTO BITEGO.RAW.reviews     FROM @BITEGO.RAW.BITEGO_RAW_STAGE/reviews/",
]

with DAG(
    dag_id="bitego_batch",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bitego", "dbt", "snowflake"],
    doc_md=__doc__,
) as dag:

    reload_raw = SQLExecuteQueryOperator(
        task_id="reload_raw", conn_id="snowflake_default",
        sql=COPY_RAW, split_statements=True, autocommit=True,
    )

    dbt_build_core = BashOperator(
        task_id="dbt_build_core",
        bash_command=f"{DBT} build --exclude tag:ai --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}",
    )

    enrich_reviews = BashOperator(
        task_id="enrich_reviews",
        bash_command=f"python /opt/airflow/ai/enrich_reviews.py",
    )

    dbt_build_ai = BashOperator(
        task_id = "dbt_build_ai",
        bash_command=f"{DBT} build --select tag:ai --project-dir {DBT_PROJECT} --profiles-dir {DBT_PROJECT}"
    )

    reload_raw >> dbt_build_core >> enrich_reviews >> dbt_build_ai