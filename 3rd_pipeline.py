from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.dates import days_ago

default_arguments = {'owner': 'José Otón', 'start_date': days_ago(1)}

with DAG(
    dag_id='3rd_exercise', 
    schedule_interval='@hourly',
    catchup=False,
    default_args=default_arguments,
) as dag:
    
    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id = 'load_data',
        bucket = 'your_bucket',
        source_objects=['data/cardata/*'],
        source_format='CSV',
        skip_leading_rows=1,
        destination_project_dataset_table='your_proyect_id.fact_project.m_history_vehicle_jose',
        create_disposition='CREATE_IF_NEEDED',#Airflow creates the table,
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    query = '''
    SELECT * EXCEPT (rank)
    FROM (
        SELECT 
            *, 
            ROW_NUMBER() OVER(
            PARTITION BY vehicle_id ORDER BY DATETIME(date, TIME(hour, minute, 0)) DESC
        ) AS rank
        FROM `your_proyect_id.fact_project.m_history_vehicle_jose`
    ) as latest
    WHERE rank = 1;

    '''
    create_table = BigQueryOperator(
        task_id='create_table',
        sql=query,
        destination_dataset_table='your_proyect_id.fact_project.m_history_vehicle_out_jose',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='US',
        bigquery_conn_id='google_cloud_default'
    )

load_data >> create_table