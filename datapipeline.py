import pandas as pd
from google.cloud import bigquery
# 2. Run
def readStorage():
    df_01 = pd.read_csv("gs://your_bucket/data/retail-data/by-day/2010-12-01.csv")
    transformData(df_01)

def transformData(df_01):
    # Calculamos la nueva variable
    df_01["Total01"] = df_01["Quantity"]*df_01["UnitPrice"]
    # Agrupamos y generamos el nuevo dataframe
    df_01_agg = pd.DataFrame(df_01.groupby(["Description"])["Total01"].agg(sum)).reset_index()
    df_01_agg.to_csv("gs://your_bucket/data/tmp/enrichedData_jose.csv",index=False)

def writeBq():
    df = pd.read_csv("gs://your_bucket/data/tmp/enrichedData_jose.csv")

    table_id = 'fact_project.upsales_byday_jose'
    # Since string columns use the "object" dtype, pass in a (partial) schema to ensure the correct BigQuery data type.
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField("Description", "STRING"),
        bigquery.SchemaField("Total01", "FLOAT")
    ])

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )

    job.result()