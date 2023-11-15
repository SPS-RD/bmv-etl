import logging
from io import StringIO

import pandas as pd

import read_data
from catalogo import campagna_sec_write, punto_vendita_write, white_list_write, black_list_write, anagrafiche_write, \
    partner_write, limits_write, pin_prefix_write, utenza_rinnovabile_write
from promozioni import promozioni_write
from promozioni.bonus import bonus_write
from promozioni.criteri import criteri_write
from promozioni.relazioni import relazioni_write
from promozioni.relazioni_bonus import relazioni_bonus_write
from promozioni.selezione_bonus import selezione_bonus_write
from ricarica import ricarica_write


def launch_etl(mongo_client, oracle_cursor):
    logging.info("ETL Start")
    etl_ricarica(mongo_client, oracle_cursor)
    etl_catalogo(mongo_client, oracle_cursor)
    logging.info("ETL Done")


def etl_catalogo(mongo_client, oracle_cursor):
    logging.info("ETL Catalogo Start")
    etl_promozioni(mongo_client, oracle_cursor)
    etl_campagna_sec(mongo_client, oracle_cursor)
    etl_punto_vendita(mongo_client, oracle_cursor)
    etl_white_list(mongo_client, oracle_cursor)
    etl_black_list(mongo_client, oracle_cursor)
    etl_anagrafiche_auth_id(mongo_client, oracle_cursor)
    etl_partner(mongo_client, oracle_cursor)
    etl_p1a_act_limits(mongo_client, oracle_cursor)
    etl_pin_prefix(mongo_client, oracle_cursor)
    etl_utenza_rinnovabile(mongo_client, oracle_cursor)
    logging.info("ETL Catalogo Done")


def etl_promozioni(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'promozioni'...")
    promozioni_data = read_data.read(oracle_cursor, 'sql/promozioni/read_promozioni.sql')
    promozioni_to_insert = pd.read_json(StringIO(promozioni_data))
    promozioni_write.write(promozioni_to_insert, mongo_client)
    # relazioni e criteri
    logging.info("Reading and writing 'relazioni' and 'criteri'. Updating previous promozioni document...")
    relazioni_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazioni.sql')
    relazioni_to_insert = pd.read_json(StringIO(relazioni_data))
    criteri_data = read_data.read(oracle_cursor, 'sql/promozioni/read_criteri.sql')
    criteri_to_insert = pd.read_json(StringIO(criteri_data))
    relazioni_write.write(relazioni_to_insert=relazioni_to_insert, criteri_to_insert=criteri_to_insert,
                          mongo_client=mongo_client)
    logging.info("Done Reading and writing 'relazioni' and 'criteri'.")
    # relazione bonus document
    logging.info(
        "Reading and writing 'bonus', 'relazioni_b', 'relazioni_bonus'. Updating previous 'promozioni' document...")
    relazioni_b_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazionib.sql')
    relazioni_bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazioni_bonus.sql')
    relazioni_bonus_to_insert = pd.read_json(StringIO(relazioni_bonus_data))
    relazioni_b_to_insert = pd.read_json(StringIO(relazioni_b_data))
    bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_bonus.sql')
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
    selezioni_bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_selezione_bonus.sql')
    selezioni_bonus_to_insert = pd.read_json(StringIO(selezioni_bonus_data))
    selezione_bonus_write.write(selezioni_bonus_to_insert, mongo_client)
    logging.info("Done Reading and Writing 'selezioni_bonus' and 'bonus'")


def etl_ricarica(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'ricarica'...")
    ricarica_data = read_data.read(oracle_cursor, 'sql/ricarica/read_ricarica.sql')
    ricarica_data_to_insert = pd.read_json(StringIO(ricarica_data))
    ricarica_write.write(ricarica_data_to_insert, mongo_client)
    logging.info("Done Reading and writing 'ricarica'.")


def etl_campagna_sec(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'campagna_sec'...")
    campagna_sec_data = read_data.read(oracle_cursor, 'sql/catalogo/campagna_sec_read.sql')
    campagna_sec_data_to_insert = pd.read_json(StringIO(campagna_sec_data))
    campagna_sec_write.write(campagna_sec_data_to_insert, mongo_client)
    logging.info("Done Reading and writing 'campagna_sec'.")


def etl_punto_vendita(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'punto_vendita'...")
    punto_vendita_data = read_data.read(oracle_cursor, 'sql/catalogo/punto_vendita_read.sql')
    punto_vendita_insert = pd.read_json(StringIO(punto_vendita_data))
    punto_vendita_write.write(punto_vendita_insert, mongo_client)
    logging.info("Done Reading and writing 'punto_vendita'.")


def etl_white_list(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'white_list'...")
    white_list_data = read_data.read(oracle_cursor, 'sql/catalogo/white_list_read.sql')
    white_list_utenti_data = read_data.read(oracle_cursor, 'sql/catalogo/white_list_utenti_read.sql')
    white_list_insert = pd.read_json(StringIO(white_list_data))
    white_list_utenti_insert = pd.read_json(StringIO(white_list_utenti_data))
    white_list_write.write(white_list_insert, white_list_utenti_insert, mongo_client)
    logging.info("Done Reading and writing 'white_list'.")


def etl_black_list(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'black_list'...")
    black_list_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_read.sql')
    black_list_utenti_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_utente_read.sql')
    black_list_multipla_ass_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_multipla_ass.sql')
    black_list_insert = pd.read_json(StringIO(black_list_data))
    black_list_utenti_insert = pd.read_json(StringIO(black_list_utenti_data))
    black_list_multipla_ass_insert = pd.read_json(StringIO(black_list_multipla_ass_data))
    black_list_write.write(black_list_insert, black_list_utenti_insert, black_list_multipla_ass_insert, mongo_client)
    black_list_multipla_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_multipla_read.sql')
    black_list_multipla_insert = pd.read_json(StringIO(black_list_multipla_data))
    black_list_write.write_multipla(black_list_multipla_insert, mongo_client)
    logging.info("Done Reading and writing 'black_list'.")


def etl_anagrafiche_auth_id(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'anagrafiche_auth'...")
    anagrafiche_data = read_data.read(oracle_cursor, 'sql/catalogo/anagrafiche_id_auth_read.sql')
    anagrafica_to_insert = pd.read_json(StringIO(anagrafiche_data))
    anagrafiche_write.write(anagrafica_to_insert, mongo_client)
    logging.info("Done Reading and writing 'anagrafiche_auth'.")


def etl_partner(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'partner'...")
    partner_data = read_data.read(oracle_cursor, 'sql/catalogo/partner_read.sql')
    partner_to_insert = pd.read_json(StringIO(partner_data))
    partner_write.write(partner_to_insert, mongo_client)
    logging.info("Done Reading and writing 'partner'.")


def etl_p1a_act_limits(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'etl_p1a_act_limits'...")
    limits_data = read_data.read(oracle_cursor, 'sql/catalogo/p1a_act_limits_read.sql')
    limits_to_insert = pd.read_json(StringIO(limits_data))
    limits_write.write(limits_to_insert, mongo_client)
    logging.info("Done Reading and writing 'etl_p1a_act_limits'.")


def etl_pin_prefix(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'pin_prefix'...")
    pin_prefix_data = read_data.read(oracle_cursor, 'sql/catalogo/pin_prefix_read.sql')
    pin_prefix_to_insert = pd.read_json(StringIO(pin_prefix_data))
    pin_prefix_write.write(pin_prefix_to_insert, mongo_client)
    logging.info("Done Reading and writing 'pin_prefix'.")


def etl_utenza_rinnovabile(mongo_client, oracle_cursor):
    logging.info("Reading and writing 'utenza_rinnovabile'...")
    utenza_rinnovabile_data = read_data.read(oracle_cursor, 'sql/catalogo/utenza_rinnovabile_read.sql')
    utenza_rinnovabile_to_insert = pd.read_json(StringIO(utenza_rinnovabile_data))
    utenza_rinnovabile_write.write(utenza_rinnovabile_to_insert, mongo_client)
    logging.info("Done Reading and writing 'utenza_rinnovabile'.")
