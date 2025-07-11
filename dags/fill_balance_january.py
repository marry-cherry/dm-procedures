from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'owner': 'marina'
}

with DAG(
    dag_id='fill_balance_january_2018',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # задача загрузки остатков на 31.12.2017
    init_balance = PostgresOperator(
        task_id='init_balance_2017_12_31',
        postgres_conn_id='postgres_default',
        sql="""
        DELETE FROM dm.dm_account_balance_f WHERE on_date = DATE '2017-12-31';

        INSERT INTO dm.dm_account_balance_f (
            on_date,
            account_rk,
            balance_out,
            balance_out_rub
        )
        SELECT
            DATE '2017-12-31',
            f.account_rk,
            f.balance_out,
            f.balance_out * COALESCE(r.reduced_cource, 1)
        FROM ds.ft_balance_f f
        LEFT JOIN ds.md_account_d acc
            ON acc.account_rk = f.account_rk
        LEFT JOIN ds.md_exchange_rate_d r
            ON r.data_actual_date = DATE '2017-12-31' AND r.currency_rk = acc.currency_rk;
        """
    )

    # Цикл по дням января
    for day in range(1, 32):
        day_str = f'2018-01-{day:02d}'
        balance_task = PostgresOperator(
            task_id=f'fill_balance_{day_str}',
            postgres_conn_id='postgres_default',
            sql=f"CALL ds.fill_account_balance_f(DATE '{day_str}');"
        )
        init_balance >> balance_task

