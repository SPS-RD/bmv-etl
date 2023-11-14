import os


def write(promozioni, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['promozioni']

    promozioni_collection.insert_many(promozioni.to_dict('records'))
