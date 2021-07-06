# STEP 1: Libraries needed
from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator


# STEP 2:Define a start date
yesterday = datetime(2021, 6, 21)#YYYY-M-D

# STEP 3: Set default arguments for the DAG
default_dag_args = {
'owner': 'José Otón',
'start_date': yesterday
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with models.DAG(
dag_id='1rst_exercise',
description='Hello World =)',
schedule_interval=timedelta(days=1),
default_args=default_dag_args) as dag:


# STEP 5: Set Operators
# BashOperator
# Every operator has at least a task_id and the other parameters are particular for each one, in this case, is a simple BashOperatator this operator will execute a simple echo “Hello World!”
    helloOp = BashOperator(
        task_id='hello_world',
        bash_command='echo "1rst Hello World!"'
    )

    helloOp2 = BashOperator(
        task_id='hello_world_2',
        bash_command='echo "2nd Hello World!"'
    )

# STEP 6: Set DAGs dependencies
# Since we have only one, we just write the operator
helloOp >> helloOp2