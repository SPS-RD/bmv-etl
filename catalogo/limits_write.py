import json

import write_data


def write(limits_to_insert):
    write_data.write_file(limits_to_insert, "p1a_act_limits.json")