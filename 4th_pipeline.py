# STEP 1: Libraries needed
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datapipeline import readStorage, transformData, writeBq
from airflow.utils.dates import days_ago
import os
# SET env vars
pathKey = "/your_local_path/key.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = pathKey

# STEP 2:Define a start date
yesterday = days_ago(1)

# STEP 3: Set default arguments for the DAG
default_dag_args = {
    'owner': 'José Otón',
    'start_date': yesterday
    #'depends_on_past': False,
    #'email_on_failure': False,
    #'email_on_retry': False,
    #'retries': 0,
    # 'retry_delay': timedelta(minutes=5)
}
# STEP 4: Define DAG
with models.DAG(
    dag_id='4th_exercise',
    description='Data Pipeline',
    schedule_interval='@weekly',#Other options timedelta(days=1) @daily
    default_args=default_dag_args) as dag:

# STEP 5: Set Operators
    # ET (Extract and Transform): PythonOperator
    read_storage = PythonOperator(
        task_id='read_storage',
        python_callable=readStorage,
        dag=dag,
    )

    # L (Load to BQ): PythonOperator
    write_bq = PythonOperator(
        task_id='write_bq',
        python_callable=writeBq,
        dag=dag,
    )

# STEP 6: Set DAGs dependencies
read_storage >> write_bq