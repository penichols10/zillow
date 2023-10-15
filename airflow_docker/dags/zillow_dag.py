from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from ZillowPythonETL import extractZillow, transformZillow, loadZillow
import os 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 14)
}
with DAG(
    dag_id='Zillow-ETL',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
) as dag:
    extract = PythonOperator(
        task_id='scrape_zillow',
        python_callable=extractZillow.scrape_results
    )
    
    transform = PythonOperator(
        task_id='clean_zillow',
        python_callable=transformZillow.clean_data
    )
    
    load = PythonOperator(
        task_id='load_flat_data',
        python_callable=loadZillow.load
    )
    
    truncate = SQLExecuteQueryOperator(
        task_id = 'truncate_for_test',
        sql=f'ZillowSQL/selecttest.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    broker = SQLExecuteQueryOperator(
        task_id='dimBroker_upsert',
        sql='ZillowSQL/broker.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    builder = SQLExecuteQueryOperator(
        task_id='dimBuilder_upsert',
        sql='ZillowSQL/builder.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    city = SQLExecuteQueryOperator(
        task_id='dimCity_upsert',
        sql='ZillowSQL/city.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    location = SQLExecuteQueryOperator(
        task_id='dimLocation_upsert',
        sql='ZillowSQL/location.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    homestatus = SQLExecuteQueryOperator(
        task_id='dimHomestatus_upsert',
        sql='ZillowSQL/homestatus.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    hometype = SQLExecuteQueryOperator(
        task_id='dimHometype_upsert',
        sql='ZillowSQL/hometype.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    newhome = SQLExecuteQueryOperator(
        task_id='dimNewHome_upsert',
        sql='ZillowSQL/newhome.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    openhouse = SQLExecuteQueryOperator(
        task_id='dimOpenhouse_upsert',
        sql='ZillowSQL/openhouse.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    preforeclosureauction = SQLExecuteQueryOperator(
        task_id='dimOPreforeclosureauction_upsert',
        sql='ZillowSQL/preforeclosureauction.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    
    fact = SQLExecuteQueryOperator(
        task_id='fact_upsert',
        sql='ZillowSQL/fact.sql',
        conn_id='zillowtest',
        database='postgres'
    )
    

extract >> transform >> load
load >> broker >> fact
load >> builder >> fact
load >> city >> location >> fact
load >> homestatus >> fact
load >> hometype >> fact
load >> newhome >> fact
load >> openhouse >> fact
load >> preforeclosureauction >> fact
