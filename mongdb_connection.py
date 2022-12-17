import pymongo
import certifi
import json
import config
import pandas as pd
import os,sys
from logger import logging
from exception import YoutubeException


# Function to create mongo db connection
def create_connection():
    try:
        logging.info("Creating Mongo DB Connection")
        config.configure()
        mongo_user = config.get_mongodb_username()
        mongo_pass = config.get_mongodb_password()
        client = pymongo.MongoClient(f"mongodb+srv://{mongo_user}:{mongo_pass}@cluster0.beyzc.mongodb.net/?retryWrites=true&w=majority")
        logging.info(f"Connection: {client}")
        return client
    except Exception as e:
        logging.debug(str(e))
        raise YoutubeException(e,sys)


# Function to store data into mongodb database
def store_data(collection_name):
    try:
        client = create_connection()
        # create database
        logging.info("Creating Database:scrap_youtube ")
        db = client['scrap_youtube']
        # create collection
        logging.info(f"Creating Connection {collection_name}")
        collection = db[collection_name]

        # loading json data
        logging.info(f"loading json data {collection_name}.json")
        with open(f'{collection_name}.json') as file:
            file_data = json.load(file)
        
        logging.info(f"storing json data {collection_name}.json into {collection}")
        # storing json data into collection
        collection.insert_many([file_data])

    except Exception as e:
        logging.debug(str(e))
        raise YoutubeException(e,sys)


# Function to fetch data from mongo db 
def find_data(collection_name):
    try:
        client = create_connection()
        logging.info("Calling the mongo db database scrap_youtube")
        db = client["scrap_youtube"]
        logging.info(f"Calling the mongo db connection {collection_name}")
        collection = db[collection_name]
        logging.info(f"Fetching data from {collection}")
        data = collection.find({})
        logging.info(f"Data={data}")

    except Exception as e:
        logging.debug(str(e))
        raise YoutubeException(e,sys)


# Function to convert data into jsonformat
def data_handling(data,table_name):
    try:
        logging.info("Converting Data Pandas dataframe")
        df = pd.DataFrame(data)
        logging.info("Converting data into json format")
        df.to_json(f"{table_name}.json", indent=False)
        
        logging.info("storing data into {table_name}")
        store_data(table_name)

    except Exception as e:
        logging.debug(str(e))
        raise YoutubeException(e,sys)

