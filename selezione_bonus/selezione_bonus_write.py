import os


def write(selezioni_bonus_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['promozioni']
    promozioni = promozioni_collection.find()

    selezioni_bonus = selezioni_bonus_to_insert.to_dict(orient='records')
    for promozione in promozioni:
        if 'relazioni' in promozione:
            for relazione in promozione['relazioni']:
                if 'criteri' in relazione:
                    for criterio in relazione['criteri']:
                        for nuova_selezione_bonus in selezioni_bonus:
                            if criterio['criterio_id'] == nuova_selezione_bonus['criterio_id']:
                                filtro = {"promozione_id": promozione['promozione_id']}
                                aggiornamento = {"$push": {"selezione_bonus": nuova_selezione_bonus}}
                                promozioni_collection.update_one(filtro, aggiornamento)
