import os

import pandas as pd


def write(utenza_rinnovabile_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['utenza_rinnovabile']
    utenza_rinnovabile_to_insert['data_lista'] = pd.to_datetime(utenza_rinnovabile_to_insert['data_lista'])
    utenza_rinnovabile_data = utenza_rinnovabile_to_insert.to_dict(orient='records')
    collection.insert_many(utenza_rinnovabile_data)