from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain, cross_downstream
from random import seed, random

default_arguments = {
    'owner': 'José Otón',
    'start_date': days_ago(1)
}

#/home/hola/airflow-tutorial/dags
#catchup by default is true, keep true if you need to run from the past
#dag = DAG('core_concepts', schedule_interval='@daily', catchup=False)
#to avoid dag=dag

with DAG(
    dag_id='2nd_exercise', 
    schedule_interval='@daily', 
    catchup=False,
    default_args=default_arguments,
) as dag:

    bash_task = BashOperator(
        task_id="bash_command", 
        bash_command='echo "Today is: \'$TODAY\' "', 
        env={'TODAY': "2021-06-21"},
    )

    def print_random_number(number):
        seed(number)
        print(random())
    
    python_task = PythonOperator(
        task_id='python_function', 
        python_callable=print_random_number, 
        op_args=[1],
    )

bash_task >> python_task