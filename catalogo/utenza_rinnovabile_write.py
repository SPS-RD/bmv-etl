import datetime

import write_data




def write(utenza_rinnovabile_to_insert):
    # utenza_rinnovabile_to_insert['data_lista'] = pd.to_datetime(utenza_rinnovabile_to_insert['data_lista'])
    # utenza_rinnovabile_to_insert = utenza_rinnovabile_to_insert.applymap(datetime_handler)
    write_data.write_file(utenza_rinnovabile_to_insert, "utenza_rinnovabile.json")
