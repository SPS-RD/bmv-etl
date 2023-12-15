import json
import os

import write_data


def write(black_list_to_insert, black_list_utenti_to_insert, black_list_multipla_ass_insert):
    black_list_data = json.loads(black_list_to_insert)
    black_list_utenti_data = json.loads(black_list_utenti_to_insert)
    black_list_multipla_ass_data = json.loads(black_list_multipla_ass_insert)

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
    write_data.write_file(black_list_data, "black_list.json")


def write_multipla(black_list_multipla_insert):
    write_data.write_file(black_list_multipla_insert, "black_list_multiple.json")
