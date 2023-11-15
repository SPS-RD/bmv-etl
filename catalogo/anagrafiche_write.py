import os


def write(anagrafice_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['anagrafiche_id_auth']
    anagrafiche_data = anagrafice_to_insert.to_dict(orient='records')
    collection.insert_many(anagrafiche_data)