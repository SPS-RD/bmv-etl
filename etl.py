import logging

from io import StringIO
import pandas as pd
from criteri import criteri_read, criteri_write
from promozioni import promozioni_read, promozioni_write
from relazioni import relazioni_read, relazioni_write
from relazioni_bonus import relazioni_bonus_read, relazioni_bonus_write
from selezione_bonus import selezione_bonus_read, selezione_bonus_write
from bonus import bonus_read, bonus_write


def launch_etl(mongo_client, oracle_cursor):
    logging.info("ETL Start")
    logging.info("Reading and writing 'promozioni'...")
    promozioni_data = promozioni_read.get_pv_promozioni(oracle_cursor)
    promozioni_to_insert = pd.read_json(StringIO(promozioni_data))
    promozioni_write.write(promozioni_to_insert, mongo_client)
    # relazioni e criteri
    logging.info("Reading and writing 'relazioni' and 'criteri'. Updating previous promozioni document...")
    relazioni_data = relazioni_read.get_pv_relazioni(oracle_cursor)
    relazioni_to_insert = pd.read_json(StringIO(relazioni_data))
    criteri_data = criteri_read.get_pv_criteri(oracle_cursor)
    criteri_to_insert = pd.read_json(StringIO(criteri_data))
    relazioni_write.write(relazioni_to_insert=relazioni_to_insert, criteri_to_insert=criteri_to_insert,
                          mongo_client=mongo_client)
    logging.info("Done Reading and writing 'relazioni' and 'criteri'.")

    # relazione bonus document
    logging.info(
        "Reading and writing 'bonus', 'relazioni_b', 'relazioni_bonus'. Updating previous 'promozioni' document...")
    relazioni_b_data = relazioni_bonus_read.get_pv_relazioni_b(oracle_cursor)
    relazioni_bonus_data = relazioni_bonus_read.get_pv_relazioni_bonus(oracle_cursor)
    relazioni_bonus_to_insert = pd.read_json(StringIO(relazioni_bonus_data))
    relazioni_b_to_insert = pd.read_json(StringIO(relazioni_b_data))
    bonus_data = bonus_read.get_pv_bonus(oracle_cursor)
    bonus_to_insert = pd.read_json(StringIO(bonus_data))
    relazioni_bonus_write.write(relazioni_bonus_to_insert, relazioni_b_to_insert, bonus_to_insert, mongo_client)
    logging.info("Done Reading and writing 'bonus', 'relazioni_b', 'relazioni_bonus'.")

    # bonus in and
    logging.info("writing 'bonusNotrel', Updating previous 'promozioni' document...")
    bonus_write.write(bonus_to_insert, mongo_client)
    logging.info("Done writing 'bonusNotrel'.")

    # criteri in and
    logging.info("writing 'criteri', Updating previous 'promozioni' document...")

    criteri_write.write(criteri_to_insert, mongo_client)
    logging.info("Done writing 'criteri'.")

    logging.info("Reading and Writing 'selezioni_bonus' and 'bonus', Updating previous 'promozioni' document...")
    selezioni_bonus_data = selezione_bonus_read.get_pv_selezione_bonus(oracle_cursor)
    selezioni_bonus_to_insert = pd.read_json(StringIO(selezioni_bonus_data))
    selezione_bonus_write.write(selezioni_bonus_to_insert, mongo_client)
    logging.info("Done Reading and Writing 'selezioni_bonus' and 'bonus'")
    logging.info("ETL Done")

