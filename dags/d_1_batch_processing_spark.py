from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd

from datetime import datetime

def fun_top_countries_get_data(**kwargs):
    postgres_conn = BaseHook.get_connection('postgres_conn')

    jdbc_url = f"jdbc:postgresql://{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
    jdbc_user = postgres_conn.login
    jdbc_password = postgres_conn.password
    
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0")\
        .master("local").appName("Pyspark_Postgres").getOrCreate()
    
    df_country = spark.read.format("jdbc")\
        .option("url",jdbc_url)\
        .option("driver","org.postgresql.Driver")\
        .option("dbtable","country")\
        .option("user",jdbc_user)\
        .option("password",jdbc_password)\
        .load()
    df_country.createOrReplaceTempView("country")

    df_city = spark.read.format("jdbc")\
        .option("url",jdbc_url)\
        .option("driver","org.postgresql.Driver")\
        .option("dbtable","city")\
        .option("user",jdbc_user)\
        .option("password",jdbc_password)\
        .load()
    df_city.createOrReplaceTempView("city")

    df_result = spark.sql('''
        Select
            country,
            Count(country) as total,
            current_date() as date
        From country as co
        inner join city as ci
            on ci.country_id = co.country_id
        group by country
    ''' )
    df_result.write.mode('append').partitionBy('date')\
        .option('compression', 'snappy')\
        .save('data_result_task_1')

def fun_top_countries_load_data(**kwargs):
    df =pd.read_parquet('data_result_task_1')
    tidb_conn = BaseHook.get_connection('tidb_conn')

    tidb_url = f"mysql+mysqlconnector://{tidb_conn.login}:{tidb_conn.password}@{tidb_conn.host}:{tidb_conn.port}/{tidb_conn.schema}"

    engine =  create_engine(tidb_url,echo = False)
    df.to_sql(name='top_country_Gusti', con=engine, if_exists='replace')

def fun_total_film_get_data(**kwargs):
    postgres_conn = BaseHook.get_connection('postgres_conn')

    jdbc_url = f"jdbc:postgresql://{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
    jdbc_user = postgres_conn.login
    jdbc_password = postgres_conn.password
    
    spark = SparkSession.builder\
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0")\
        .master("local").appName("Pyspark_Postgres").getOrCreate()
    
    df_film = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "film") \
        .option("user", jdbc_user) \
        .option("password",jdbc_password) \
        .load()
    df_film.createOrReplaceTempView("film")

    df_film_category = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "film_category") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .load()
    df_film_category.createOrReplaceTempView("film_category")

    df_category = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "category") \
        .option("user",jdbc_user) \
        .option("password",jdbc_password) \
        .load()
    df_category.createOrReplaceTempView("category")

    df_result_2 = spark.sql('''
      SELECT
        name,
        COUNT(name) as total,
        current_date() as date
      FROM (
        SELECT * FROM film AS f
        JOIN film_category as fc
        ON f.film_id = fc.film_id) AS j
      JOIN category AS c ON j.category_id = c.category_id
      GROUP BY name ORDER BY COUNT(name) DESC
    ''')
    df_result_2.write.mode('overwrite').partitionBy('date')\
        .option('compression', 'snappy')\
        .save('data_result_task_2')

def fun_total_film_load_data(**kwargs):
    df =pd.read_parquet('data_result_task_2')
    tidb_conn = BaseHook.get_connection('tidb_conn')

    tidb_url = f"mysql+mysqlconnector://{tidb_conn.login}:{tidb_conn.password}@{tidb_conn.host}:{tidb_conn.port}/{tidb_conn.schema}"

    engine =  create_engine(tidb_url,echo = False)
    df.to_sql(name='total_film_Gusti', con=engine, if_exists='replace')

with DAG(
    dag_id='d_1_batch_processing_spark',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    op_top_countries_get_data = PythonOperator(
        task_id="top_countries_get_data",
        python_callable=fun_top_countries_get_data
    )

    op_top_countries_load_data = PythonOperator(
        task_id="top_countries_load_data",
        python_callable=fun_top_countries_load_data
    )

    op_total_film_get_data = PythonOperator(
        task_id="total_film_get_data",
        python_callable=fun_total_film_get_data
    )

    op_total_film_load_data = PythonOperator(
        task_id="total_film_load_data",
        python_callable=fun_total_film_load_data
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    start_task >> op_top_countries_get_data >> op_top_countries_load_data >> end_task
    start_task >> op_total_film_get_data >> op_total_film_load_data >> end_task