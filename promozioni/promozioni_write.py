import json
import logging
import os

import write_data


def write(promozioni, mongo_client):
    mongo_db = mongo_client[os.environ.get('MONGO_DB_NAME')]
    promozioni_collection = mongo_db['promozioni']

    promozioni_collection.insert_many(promozioni.to_dict('records'))


def new_write(promozioni, bonuss, criteri, relazioni, relazioni_bonus, relazioni_b, selezioni_bonus):
    promozioni_data = json.loads(promozioni)
    bonus_data = json.loads(bonuss)
    criteri_data = json.loads(criteri)
    relazioni_data = json.loads(relazioni)
    relazioni_bonus_data = json.loads(relazioni_bonus)
    relazioni_b_data = json.loads(relazioni_b)
    selezioni_bonus_data = json.loads(selezioni_bonus)

    split_indexes = list(range(99, len(promozioni_data), 100))
    for i, promozione in enumerate(promozioni_data):
        logging.info(f"working #promozione: {i} out of {len(promozioni_data)}")
        for bonus in bonus_data:
            if promozione['promozione_id'] == bonus['promozione_id']:
                if 'bonusNotRel' in promozione:
                    promozione['bonusNotRel'].append(bonus)
                else:
                    promozione['bonusNotRel'] = [bonus]
        for criterio in criteri_data:
            if promozione['promozione_id'] == criterio['promozione_id']:
                if 'criteri' in promozione:
                    promozione['criteri'].append(criterio)
                else:
                    promozione['criteri'] = [criterio]
        for nuova_relazione in relazioni_data:
            for criterio in criteri_data:
                if nuova_relazione['promozione_id'] == criterio['promozione_id']:
                    if 'criteri' in nuova_relazione:
                        nuova_relazione['criteri'].append(criterio)
                    else:
                        nuova_relazione['criteri'] = [criterio]
            if promozione['promozione_id'] == nuova_relazione['promozione_id']:
                if 'relazioni' in promozione:
                    promozione['relazioni'].append(nuova_relazione)
                else:
                    promozione['relazioni'] = [nuova_relazione]
        for nuova_relazione_b in relazioni_b_data:
            for nuova_relazione_bonus in relazioni_bonus_data:
                if nuova_relazione_b['relazioneb_id'] == nuova_relazione_bonus['relazione_bonus_id']:
                    for nuovo_bonus in bonus_data:
                        if nuovo_bonus['bonus_id'] == nuova_relazione_bonus['bonus_id']:
                            # and nuova_relazione_b['promozione_id'] == nuovo_bonus['promozione_id']:
                            if 'bonus' in nuova_relazione_bonus:
                                nuova_relazione_bonus['bonus'].append(nuovo_bonus)
                            else:
                                nuova_relazione_bonus['bonus'] = [nuovo_bonus]
                if promozione['promozione_id'] == nuova_relazione_b['promozione_id']:
                    if 'relazioni' in promozione:
                        promozione['relazioni_bonus'].append(nuova_relazione_bonus)
                    else:
                        promozione['relazioni_bonus'] = [nuova_relazione_bonus]

        if 'relazioni' in promozione:
            for relazione in promozione['relazioni']:
                if 'criteri' in relazione:
                    for criterio in relazione['criteri']:
                        for nuova_selezione_bonus in selezioni_bonus_data:
                            if criterio['criterio_id'] == nuova_selezione_bonus['criterio_id']:
                                if 'selezione_bonus' in promozione:
                                    promozione['selezione_bonus'].append(nuova_selezione_bonus)
                                else:
                                    promozione['selezione_bonus'] = [nuova_selezione_bonus]
        if i in split_indexes:  # i%100 ranges from 0 to 99, so if it's 99, that means we've finished 100 records
            filename = f'promozioni_{split_indexes.index(i)}.json'  # Generate a new filename based on how many
            # batches of 100 items we've finished
            write_data.write_file(promozioni_data[(i - 99):(i + 1)], filename)  # Write to the file

    # Write remaining records, if there are any
    remaining_records_start = split_indexes[-1] + 1 if split_indexes else 0
    if remaining_records_start < len(promozioni_data):
        filename = f'promozioni_{len(split_indexes)}.json'  # This will be the next file in the sequence
        write_data.write_file(promozioni_data[remaining_records_start:], filename)
    logging.info("Data written to files.")
