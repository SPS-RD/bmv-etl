import os


def write(relazioni_to_insert, criteri_to_insert,mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['promozioni']
    nuove_relazioni = relazioni_to_insert.to_dict(orient='records')
    nuovi_criteri = criteri_to_insert.to_dict(orient='records')
    for nuova_relazione in nuove_relazioni:
        for criterio in nuovi_criteri:
            if nuova_relazione['promozione_id'] == criterio['promozione_id']:
                if 'criteri' in nuova_relazione:
                    nuova_relazione['criteri'].append(criterio)
                else:
                    nuova_relazione['criteri'] = [criterio]
        filtro = {"promozione_id": nuova_relazione['promozione_id']}
        aggiornamento = {"$push": {"relazioni": nuova_relazione}}
        promozioni_collection.update_one(filtro, aggiornamento)
