import json


def write_file(data, filename):
    # data = map_boolean(
    #     ['flag_evento_rilevato', 'flag_utente_coop', 'flag_invio_notifica', 'flag_porta_un_amico', 'flag_white_list'],
    #     data)
    if isinstance(data, list):
        data = json.dumps(data)
    with open('collections/' + filename, 'w') as json_file:
        json_file.write(data)


def map_boolean(fields, data):
    if isinstance(data, str):
        data_list = json.loads(data)
    elif isinstance(data, dict):
        data_list = data
    elif isinstance(data, list):
        data_list = data
    else:
        raise ValueError("The data should be a dictionary or a JSON string")
    for entry in data_list:
        for field in fields:
            if field in entry:
                if entry[field] == 'T':
                    entry[field] = True
                else:
                    entry[field] = False
    return json.dumps(data_list)
