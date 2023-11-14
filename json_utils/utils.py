import json

def converti_a_minuscolo(json_data: str) -> str:
    # Analizza il JSON in un dizionario
    data_list = json.loads(json_data)

    # Converte tutte le chiavi di ciascun dizionario in lettere minuscole
    data_list_lower = [{key.lower(): value for key, value in item.items()} for item in data_list]

    # Ritorna la lista di dizionari con chiavi in lettere minuscole
    return json.dumps(data_list_lower)
