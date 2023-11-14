import os

def write(bonus_to_insert, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['promozioni']
    nuovi_bonus = bonus_to_insert.to_dict(orient='records')
    for nuovo_bonus in nuovi_bonus:
        filtro = {"promozione_id": nuovo_bonus['promozione_id']}
        aggiornamento = {"$push": {"bonusNotRel": nuovo_bonus}}
        promozioni_collection.update_one(filtro, aggiornamento)
