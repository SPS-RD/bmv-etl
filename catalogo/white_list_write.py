import json

import write_data


def write(white_list_to_insert, white_list_utenti_to_insert):
    white_list_data = json.loads(white_list_to_insert)
    white_list_utenti_data = json.loads(white_list_utenti_to_insert)

    for whitelist in white_list_data:
        for utente in white_list_utenti_data:
            if whitelist['white_list_id'] == utente['white_list_id']:
                if 'white_list_utenti' in whitelist:
                    whitelist['white_list_utenti'].append(str(utente['msisdn']))
                else:
                    whitelist['white_list_utenti'] = [str(utente['msisdn'])]

    write_data.write_file(white_list_data, "white_list.json")

