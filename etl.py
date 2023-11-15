import logging

from io import StringIO
import pandas as pd
from promozioni.criteri import criteri_write
from promozioni.criteri import criteri_read
from promozioni import promozioni_read, promozioni_write
from promozioni.relazioni import relazioni_read, relazioni_write
from promozioni.relazioni_bonus import relazioni_bonus_write, relazioni_bonus_read
from promozioni.selezione_bonus import selezione_bonus_write, selezione_bonus_read
from promozioni.bonus import bonus_read, bonus_write
from catalogo import campagna_sec_read, campagna_sec_write, punto_vendita_read, punto_vendita_write
from ricarica import ricarica_read, ricarica_write


def launch_etl(mongo_client, oracle_cursor):
    logging.info("ETL Start")
    etl_promozioni(mongo_client, oracle_cursor)
    etl_ricarica(mongo_client, oracle_cursor)
    etl_campagna_sec(mongo_client, oracle_cursor)
    etl_punto_vendita(mongo_client, oracle_cursor)
    logging.info("ETL Done")


def etl_promozioni(mongo_client, oracle_cursor):
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


def etl_ricarica(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'ricarica'...")
    ricarica_data = ricarica_read.get_pv_ivr_ricarica(oracle_cursor)
    ricarica_data_to_insert = pd.read_json(StringIO(ricarica_data))
    ricarica_write.write(ricarica_data_to_insert, mongo_client)
    logging.info("Done Reading and writing 'ricarica'.")


def etl_campagna_sec(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'campagna_sec'...")
    campagna_sec_data = campagna_sec_read.get_pv_campagna_sec(oracle_cursor)
    campagna_sec_data_to_insert = pd.read_json(StringIO(campagna_sec_data))
    campagna_sec_write.write(campagna_sec_data_to_insert, mongo_client)
    logging.info("Done Reading and writing 'campagna_sec'.")


def etl_punto_vendita(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'punto_vendita'...")
    punto_vendita_data = punto_vendita_read.get_pv_puntovendita(oracle_cursor)
    punto_vendita_insert = pd.read_json(StringIO(punto_vendita_data))
    punto_vendita_write.write(punto_vendita_insert, mongo_client)
    logging.info("Done Reading and writing 'punto_vendita'.")
