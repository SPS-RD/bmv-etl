import os
def write(ricariche_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['ivr_ricariche']

    ricariche = ricariche_to_insert.to_dict(orient='records')
    promozioni_collection.insert_many(ricariche)
