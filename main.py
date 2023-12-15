import logging
import os
import platform

import oracledb
from dotenv import load_dotenv
from oracledb import Cursor
from pymongo import MongoClient

import etl


def get_mongo_client() -> MongoClient:
    mongo_host = os.environ.get('MONGO_HOST')
    mongo_port = os.environ.get('MONGO_PORT')
    mongo_user = os.environ.get('MONGO_USERNAME')
    mongo_pwd = os.environ.get('MONGO_PASSWORD')
    mongo_client = MongoClient(f"mongodb://{mongo_user}:{mongo_pwd}@{mongo_host}:{mongo_port}")
    return mongo_client


def get_oracle_cursor() -> Cursor:
    db_username = os.environ.get('ORACLE_USERNAME')
    db_password = os.environ.get('ORACLE_PASSWORD')
    db_host = os.environ.get('ORACLE_HOST')
    db_port: int | None = os.environ.get('ORACLE_PORT')
    db_service = os.environ.get('ORACLE_SERVICE')

    # Crea la stringa di connessione per Oracle
    oracle_folder = platform.system()
    if oracle_folder == 'Linux':
        oracledb.init_oracle_client()
    else:
        oracledb.init_oracle_client(lib_dir=f"{oracle_folder}/instantclient_21_12")
    # Connessione al database Oracle
    oracle_connection = oracledb.connect(
        user=db_username, password=db_password, host=db_host, port=db_port, sid=db_service
    )
    return oracle_connection.cursor()


def main():
    load_dotenv()
    logging.basicConfig(level=logging.DEBUG,  # Imposta il livello di registrazione minimo
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info("Launching ETL")
    try:
        oracle_cursor = get_oracle_cursor()
        # mongo_client = get_mongo_client()
        etl.launch_etl(oracle_cursor=oracle_cursor)

    finally:
        # Chiudi i cursori e le connessioni
        logging.info("Closing database connections")
        oracle_cursor.close()
        logging.info("Done.")

if __name__ == "__main__":
    main()
