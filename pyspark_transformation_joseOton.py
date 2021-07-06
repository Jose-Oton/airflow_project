import sys
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
            .appName('DataIngestion')\
            .getOrCreate()

    print("[1] ------- Spark Session Created")

    bucket = 'your_bucket'
    spark.conf.set('temporaryGcsBucket', bucket)

    print("[2] ------- Reading bq tables")

    table = "bigquery-public-data:covid19_ecdc.covid_19_geographic_distribution_worldwide"
    month = '3'
    df_covid_latam = spark.read \
                          .format("bigquery") \
                          .option("table", table) \
                          .option("filter", "month = "+ month) \
                          .load()

    print("[3] ------- Transformation ")

    # Seleccionar las columnas a usar, filtrar el pais recibido y poner en cache 
    country = 'Colombia'
    df_covid_subset = df_covid_latam \
                      .select("date", "day", "month", "year", "countries_and_territories","daily_confirmed_cases", "daily_deaths") \
                      .where("countries_and_territories IN ('"+country+"')") \
                      .cache()

    # Escribir en una tabla de BigQuery

    # Es necesario tener un dataset
    bq_dataset = 'fact_project'

    # BigQuery table que se crear¡ o se sobreescribir¡
    bq_table = 'jose' + '_covid_' + country.lower()

    print("[4] ------- Saving bq tables")

    df_covid_subset.write \
                   .format("bigquery") \
                   .option("table","{}.{}".format(bq_dataset, bq_table)) \
                   .option("temporaryGcsBucket", bucket) \
                   .mode('overwrite') \
                   .save()
  
main()