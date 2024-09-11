# import all required libraries

import datetime
import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
from ETL import data_extraction,data_transformation,data_loading
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.datas import dats_ago

logging.basicConfig(filename=f'C:\\Users\\kesha\\airflow_learning\\logs\\extraction_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}',level=logging.INFO, filemode = 'w', format='%(process)d-%(levelname)s-%(message)s')

default_args = {'owner': 'Airflow','retries':2,'retry_delay': datetime.timedelta(minutes = 10)}

with DAG( dag_id = "etl_pipeline",
    default_args = default_args,
    description = "ETL pipeline for an Airflow assignment",
    schedule_interval = '9 */3 ***',
    start_time = datetime(2024,9,9),
    catchup = False) as dag:
    def etl_pipeline():
        """
        Function to combine extraction ,transform and load process
        """
        logging.info("Start the ETL pipeline")
        extracted_data = data_extraction("customers, orders, order_items, products, categories, reviews")
        transform_data = data_transformation(extracted_data)
        data_loading(transform_data, transform_data, 'transform_data')
        data_loading(transform_data, transform_data, 'data_insights')
        logging.info("End the ETL pipeline")
    
    jobs = PythonOperator(task_id = 'etl_job', python_callable = etl_pipeline)

    jobs