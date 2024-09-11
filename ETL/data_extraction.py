import pandas as pd
from sqlalchemy import create_engine
import logging
import datetime

logging.basicConfig(filename=f'C:\\Users\\kesha\\airflow_learning\\logs\\extraction_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}',level=logging.INFO, filemode = 'w', format='%(process)d-%(levelname)s-%(message)s')

def data_extraction(tbl_names_list):
    """
    Args:
      tbl_names_list (list) : contains the string of tables for which extraction is needed as a comma seperated value
    Returns:
      Returns the dictionary containing table name as a key and pandas dataframe as a value.
    """
    tbl_names_list = list(map(lambda x : x.strip(),tbl_names_list.split(",")))
    logging.info(f"Table names : {tbl_names_list}")
    sqlserver_engine = create_engine("mssql+pyodbc://@sunflower/AIRFLOW_ASSIGNMENT?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
    tables_df_dict = {}
    for tbl in tbl_names_list:
      tables_df_dict[tbl] = pd.read_sql(f'SELECT * FROM {tbl}',sqlserver_engine)
      logging.info(f"Extraction is completed for {tbl}")
    return tables_df_dict


if __name__ == "__main__":
   logging.info("Start the data extraction process")
   tables_df_dict = data_extraction("customers, orders, order_items, products, categories, reviews")
   logging.info(f"List of tables dataframe : {tables_df_dict}")
   logging.info("Data extraction process is completed")
   