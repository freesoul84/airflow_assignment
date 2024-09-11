import pymongo
import pandas as pd
import logging

def conversion(dictionary):
    if isinstance(dictionary, pd.DataFrame):
        return dictionary.to_dict(orient='records')
    elif isinstance(dictionary, dict):
        return {k: conversion(v) for k, v in dictionary.items()}
    else:
        return dictionary

def data_loading(transformation_op, data, collection_name):    
    cli = pymongo.MongoClient("mongodb://localhost:27017/")
    database_name = cli['AIRFLOW_LEARNING']
    collection = database_name[collection_name]
    converted_data = conversion(data)
    collection.insert_one(converted_data)
    for doc in database_name.data_transform.find():
        print(doc)