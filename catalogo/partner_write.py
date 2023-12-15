import json
import os

import write_data


def write(partner_to_insert):
    write_data.write_file(partner_to_insert, "partner.json")