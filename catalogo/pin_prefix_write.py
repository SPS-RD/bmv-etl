import os


def write(pin_prefix_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['pin_prefix']
    pin_prefix_data = pin_prefix_to_insert.to_dict(orient='records')
    collection.insert_many(pin_prefix_data)