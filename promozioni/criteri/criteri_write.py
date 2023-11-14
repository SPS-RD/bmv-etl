import os
def write(criteri_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['promozioni']
    nuovi_criteri = criteri_to_insert.to_dict(orient='records')
    for nuovo_criterio in nuovi_criteri:
        filtro = {"promozione_id": nuovo_criterio['promozione_id']}
        aggiornamento = {"$push": {"criteri": nuovo_criterio}}
        promozioni_collection.update_one(filtro, aggiornamento)
