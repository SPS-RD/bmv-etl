import os
def write(punto_vendita_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['punto_vendita']

    punto_vendita_data = punto_vendita_to_insert.to_dict(orient='records')
    collection.insert_many(punto_vendita_data)