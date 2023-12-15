import write_data


def write(campagna_sec_to_insert):
    # campagna_sec_data = campagna_sec_to_insert.to_dict(orient='records')
    write_data.write_file(campagna_sec_to_insert, "campagna_sec_to_write.json")