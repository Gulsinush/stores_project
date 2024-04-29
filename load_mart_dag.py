from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

LOAD_PART_TABLE_TRAFFIC = "select std6_52.f_traffic('std6_52.traffic', 'date', '2021-01-01', '2021-02-28', 'gp.traffic', 'intern', 'intern')"
LOAD_PART_TABLE_BILLS_HEAD = "select std6_52.f_bills('std6_52.bills_head', 'calday', '2021-01-01', '2021-02-28', 'gp.bills_head', 'intern', 'intern')"
LOAD_PART_TABLE_BILLS_ITEM = "select std6_52.f_bills('std6_52.bills_item', 'calday', '2021-01-01', '2021-02-28', 'gp.bills_item', 'intern', 'intern')"
LOAD_MART = "select std6_52.f_mart('20210101', '2 month')"


DB_CONN = "gp_std6_52"
DB_SCHEMA = "std6_52"
DB_PROC_LOAD = 'f_load_full_ip'
FULL_LOAD_TABLES = ['coupons', 'promos', 'promo_types', 'stores']
FULL_LOAD_FILES = {'coupons':'coupons', 'promos':'promos', 'promo_types':'promo_types', 'stores':'stores'}
MD_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD}(%(tab_name)s, %(file_name)s);"



default_args = {
    'depends_on_past': False,
    'owner': 'std6_52',
    'start_date': datetime(2024, 4, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    "std6_52_project",
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    
    task_start = DummyOperator(task_id="start")
    
    task_load_part_traffic = PostgresOperator(task_id="load_part_traffic",
                                       postgres_conn_id=DB_CONN,
                                       sql=LOAD_PART_TABLE_TRAFFIC
                                   )
    
    task_load_part_bills_head = PostgresOperator(task_id="load_part_bills_head",
                                      postgres_conn_id=DB_CONN,
                                      sql=LOAD_PART_TABLE_BILLS_HEAD
                                   )
    task_load_part_bills_item = PostgresOperator(task_id="load_part_bills_item",
                                      postgres_conn_id=DB_CONN,
                                      sql=LOAD_PART_TABLE_BILLS_ITEM
                                   )
    
    with TaskGroup("full_load") as task_full_insert_tables:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=DB_CONN,
                                    sql=MD_TABLE_LOAD_QUERY,
                                    parameters={'tab_name':f'{DB_SCHEMA}.{table}', 
                                                'file_name':f'{FULL_LOAD_FILES[table]}'}
                                   )

    task_load_mart = PostgresOperator(task_id="load_mart",
                                       postgres_conn_id=DB_CONN,
                                       sql=LOAD_MART    
                                     )
            
    
    
    task_end = DummyOperator(task_id="end")
    
    task_start >> task_full_insert_tables >> task_load_part_traffic >> task_load_part_bills_head >> task_load_part_bills_item >> task_load_mart >> task_end