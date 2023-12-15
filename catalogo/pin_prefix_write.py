import os

import write_data


def write(pin_prefix_to_insert):
    write_data.write_file(pin_prefix_to_insert, "pin_prefix.json")