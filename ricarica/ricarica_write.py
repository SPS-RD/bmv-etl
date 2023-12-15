import os

import write_data


def write(ricariche_to_insert):
    write_data.write_file(ricariche_to_insert, "ricarica.json")
