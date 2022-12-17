import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from logger import logging
from exception import YoutubeException
import os,sys

def store_data_into_sql(user, password, data, table_name):
    global db_connection
    try:
        logging.info("Creating MySQL Connection")
        mydb = mysql.connector.connect(
            host = "localhost",
            user = user,
            password = password
        )
        logging.info(f"Successfully Established MySql Connection: {mydb}")
    except Exception as e:
        logging.debug(f"Error In Making SQL Connection: {str(e)}")
        raise YoutubeException(e,sys)


    logging.info("Setting Cursor")
    cursor = mydb.cursor()
    try:
        logging.info(f"Creating Database youtube_scrapper if not exist")
        cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_scrapper")
        cursor.execute("USE youtube_scrapper")
        logging.info(f"Dropping Table: {table_name}")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    except Exception as e:
        logging.debug(str(e))
        raise YoutubeException(e,sys)


    logging.info("Creating Connection String")
    conn_str = f'mysql+pymysql://{user}:{password}@localhost:3306/youtube_scrapper'
    try:
        db_connection = create_engine(conn_str)
        logging.info("Connection with sqlalchemy successfully established ")
        logging.info(f"Connection= {db_connection}")
    except Exception as e:
        logging.debug(f"Error occured in sqlalchemy connection string: {str(e)}")
        raise YoutubeException(e,sys)


    logging.info("Storing Data into MySql Database")
    try:
        logging.info("Creating Pandas Dataframe")
        df = pd.DataFrame(data)
        logging.info("Dropping comments column")
        df.drop("comments",axis=1,inplace=True)
        logging.info("Storing Data into MySql Database")
        df.to_sql(f"{table_name}",
            con=db_connection,
            index=False
        )
    except Exception as e:
        logging.debug(f"Exception in Dataframe: {str(e)}")
        raise YoutubeException(e,sys)