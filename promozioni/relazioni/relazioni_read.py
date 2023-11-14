import pandas as pd
from oracledb import Cursor
from pandas import DataFrame

from json_utils import utils


def get_pv_relazioni(oracle_cursor: Cursor) -> str:
    with open('sql/promozioni/read_relazioni.sql', 'r') as file:
        sql_query = file.read()

    oracle_cursor.execute(sql_query)
    columns = [desc[0] for desc in oracle_cursor.description]
    results: DataFrame = pd.DataFrame(oracle_cursor.fetchall(), columns=columns)
    json_data = results.to_json(orient='records', default_handler=str)
    return_data = utils.converti_a_minuscolo(json_data)
    return return_data