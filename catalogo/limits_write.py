import os


def write(limits_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['p1a_act_limits']
    limits_data = limits_to_insert.to_dict(orient='records')
    collection.insert_many(limits_data)