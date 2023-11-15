import os


def write(white_list_to_insert, white_list_utenti_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['white_list']

    white_list_data = white_list_to_insert.to_dict(orient='records')
    white_list_utenti_data = white_list_utenti_to_insert.to_dict(orient='records')

    for whitelist in white_list_data:
        for utente in white_list_utenti_data:
            if whitelist['white_list_id'] == utente['white_list_id']:
                if 'white_list_utenti' in whitelist:
                    whitelist['white_list_utenti'].append(str(utente['msisdn']))
                else:
                    whitelist['white_list_utenti'] = [str(utente['msisdn'])]
    collection.insert_many(white_list_data)
