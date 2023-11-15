import os


def write(black_list_to_insert, black_list_utenti_to_insert, black_list_multipla_ass_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['black_list']

    black_list_data = black_list_to_insert.to_dict(orient='records')
    black_list_utenti_data = black_list_utenti_to_insert.to_dict(orient='records')
    black_list_multipla_ass_data = black_list_multipla_ass_insert.to_dict(orient='records')

    for black_list in black_list_data:
        for utente in black_list_utenti_data:
            if black_list['black_list_id'] == utente['black_list_id']:
                if 'black_list_utenti' in black_list:
                    black_list['black_list_utenti'].append(utente)
                else:
                    black_list['black_list_utenti'] = [utente]
        for black_list_multipla in black_list_multipla_ass_data:
            if black_list['black_list_id'] == black_list_multipla['black_list_id']:
                if 'black_list_multiple_ass' in black_list:
                    black_list['black_list_multiple_ass'].append(black_list_multipla)
                else:
                    black_list['black_list_multiple_ass'] = [black_list_multipla]
    collection.insert_many(black_list_data)


def write_multipla(black_list_multipla_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    collection = mongo_db['black_list_multiple']
    black_list_multipla_data = black_list_multipla_insert.to_dict(orient='records')
    collection.insert_many(black_list_multipla_data)
