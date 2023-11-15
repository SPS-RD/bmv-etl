import os
def write(campagna_sec_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['campagne_sec']

    campagna_sec_data = campagna_sec_to_insert.to_dict(orient='records')
    promozioni_collection.insert_many(campagna_sec_data)