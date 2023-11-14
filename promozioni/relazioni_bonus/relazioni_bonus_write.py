import os


def write(relazioni_bonus_to_insert, relazioni_b_to_insert, bonus_to_insert,  mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['promozioni']
    nuove_relazioni_bonus = relazioni_bonus_to_insert.to_dict(orient='records')
    nuove_relazioni_b = relazioni_b_to_insert.to_dict(orient='records')
    nuovi_bonus = bonus_to_insert.to_dict(orient='records')

    for nuova_relazione_b in nuove_relazioni_b:
        for nuova_relazione_bonus in nuove_relazioni_bonus:
            if nuova_relazione_b['relazioneb_id'] == nuova_relazione_bonus['relazione_bonus_id']:
                for nuovo_bonus in nuovi_bonus:
                    if nuovo_bonus['bonus_id'] == nuova_relazione_bonus['bonus_id']:
                        # and nuova_relazione_b['promozione_id'] == nuovo_bonus['promozione_id']:
                        if 'bonus' in nuova_relazione_bonus:
                            nuova_relazione_bonus['bonus'].append(nuovo_bonus)
                        else:
                            nuova_relazione_bonus['bonus'] = [nuovo_bonus]
            #mancano descrizione e tipo relazione
            nuova_relazione_bonus['descrizione'] = nuova_relazione_b['descrizione']
            nuova_relazione_bonus['tipo_relazione'] = nuova_relazione_b['tipo_relazione']
            filtro = {"promozione_id": nuova_relazione_b['promozione_id']}
            aggiornamento = {"$push": {"relazioni_bonus": nuova_relazione_bonus}}
            promozioni_collection.update_one(filtro, aggiornamento)