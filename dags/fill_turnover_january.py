from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'marina'
}

with DAG(
    dag_id='fill_turnover_january_2018',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    for day in range(1, 32):
        day_str = f'2018-01-{day:02d}'
        turnover_task = PostgresOperator(
            task_id=f'fill_turnover_{day_str}',
            postgres_conn_id='postgres_default',
            sql=f"CALL ds.fill_account_turnover_f(DATE '{day_str}');"
        )

