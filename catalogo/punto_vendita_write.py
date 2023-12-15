import write_data


def write(punto_vendita_to_insert):
    write_data.write_file(punto_vendita_to_insert, "punto_vendita.json")