import os

import pandas as pd

import write_data


def write(utenza_rinnovabile_to_insert):
    # utenza_rinnovabile_to_insert['data_ultima_modifica'] = pd.to_datetime(utenza_rinnovabile_to_insert['data_ultima_modifica'])
    # utenza_rinnovabile_to_insert['is_utente_esterno'] = utenza_rinnovabile_to_insert['is_utente_esterno'].map({'f': False, 't': True})
    # utenza_rinnovabile_data = utenza_rinnovabile_to_insert.to_dict(orient='records')
    write_data.write_file(utenza_rinnovabile_to_insert, 'utenza_rinnovabile.json')