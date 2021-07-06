#1. Documentación de un DAG
"""
## PYSPARK DAG
Este pipeline toma data de Covid compartida de forma pública por Google y calcula unos KPIs.
"""

from airflow import DAG

from datetime import timedelta, datetime
from airflow.utils.dates import days_ago

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

from airflow.utils import trigger_rule
# DataprocSubmitPySparkJobOperator(
#          task_id="store_stock",
#          main="gs://your_bucket/datapipelines/pyspark/pyspark_transformation_joseOton.py",
#          cluster_name="spark-cluster-{{ ds_nodash }}",
#          dataproc_jars=["gs://spark-lib/bigquery/spark-bigquery-latest.jar"], #JAR para que Spark pueda leer de BigQuery
#          region='us-central1',
#         gcp_conn_id='google_cloud_default'
#      ).generate_job()

#2. Utilizar Variables
PROJECT_ID = Variable.get("project")
STORAGE_BUCKET = Variable.get("storage_bucket")

default_dag_args = {
    "start_date": days_ago(1),
    "owner": "José Otón"
}

def is_weekend(execution_date=None):
    date = datetime.strptime(execution_date, "%Y-%m-%d")

    if date.isoweekday() < 6:
        return "store_stock"

    return "weekend"

# DEFINIMOS DAG
with DAG(
    dag_id='5th_exercise',
    description='Running a PySpark Job on GCP',
    schedule_interval='@daily',
    default_args=default_dag_args,
    max_active_runs=1,
    user_defined_macros={"project": PROJECT_ID},#5. Macros en Airflow
) as dag:

    dag.doc_md = __doc__ #Para documentar un DAG

    create_dataproc = DataprocCreateClusterOperator(
        task_id="create_dataproc",
        project_id='{{ project }}',
        cluster_name="spark-cluster-{{ ds_nodash }}",
        num_workers=2,
        storage_bucket=STORAGE_BUCKET,
        region="us-central1"
            )

    create_dataproc.doc_md = """## Crear cluster de Dataproc
    Crea un cluster de Dataproc en el proyecto de GCP
    """

    # 3. Agregar elementos de lógica para ejecutar uno u otro pipeline
    do_analytics = BranchPythonOperator(
        task_id="do_analytics",
        python_callable=is_weekend,
        op_kwargs={"execution_date": "{{ ds }}"}, # 4. Jinja Templating
    )
    do_analytics.doc_md = """## Evalua que dia de la semana es
    Crea un cluster de Dataproc en el proyecto de GCP.
    """

    store_stock = DataprocSubmitJobOperator(
        task_id="store_stock",
        project_id=PROJECT_ID,
        location='us-central1',
           job={
            'reference': {'project_id': '{{ project }}',
            'job_id': '{{task.task_id}}_{{ds_nodash}}_2446afcc_joseOton'}, ## si puede haber cambio.
            'placement': {'cluster_name': 'spark-cluster-{{ ds_nodash }}'},
            'labels': {'airflow-version': 'v2-1-0'},
            'pyspark_job': {
                'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'],
                'main_python_file_uri': 'gs://your_bucket/datapipelines/pyspark/pyspark_transformation_joseOton.py'
            }
        },
        gcp_conn_id='google_cloud_default'          
    )

    store_stock.doc_md = """## Spark Transformation
    Ejecuta las transformaciones con Spark.
    """

    weekend = BashOperator(
        task_id="weekend", 
        bash_command='echo "\'$TODAY\' is weekend so the pipeline hasnt been executed."',
        env={'TODAY': '2021-06-20'},
    )
    weekend.doc_md = """## Imprime el día de la semana
    Se ejecuta en caso sea fin de semana.
    """

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name="spark-cluster-{{ ds_nodash }}",
        trigger_rule="all_done",
        region='us-central1'
        #zone='us-central1-a'
    )
    delete_cluster.doc_md = """## Borrar Cluster de Dataproc
    Elimina el cluster de Dataproc.
    """

# SETEAR LAS DEPEDENDENCIAS DEL DAG
    (create_dataproc >> 
    do_analytics >> [
    store_stock,
    weekend,
] >> delete_cluster)
