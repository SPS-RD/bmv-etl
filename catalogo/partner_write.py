import os


def write(partner_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['partner']
    partner_data = partner_to_insert.to_dict(orient='records')
    collection.insert_many(partner_data)