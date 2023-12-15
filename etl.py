import logging

import read_data
import write_data
from catalogo import white_list_write, black_list_write, anagrafiche_write, \
    partner_write, limits_write, pin_prefix_write, utenza_rinnovabile_write, stato_utente_write, campagna_sec_write, \
    punto_vendita_write
from promozioni import promozioni_write
from ricarica import ricarica_write


def launch_etl(oracle_cursor):
    logging.info("ETL Start")
    etl_ricarica(oracle_cursor)
    etl_catalogo(oracle_cursor)
    etl_iniziativa(oracle_cursor)
    logging.info("ETL Done")


def etl_catalogo(oracle_cursor):
    logging.info("ETL Catalogo Start")
    # etl_promozioni(oracle_cursor)
    etl_campagna_sec(oracle_cursor)
    etl_punto_vendita(oracle_cursor)
    etl_white_list(oracle_cursor)
    etl_black_list(oracle_cursor)
    etl_anagrafiche_auth_id(oracle_cursor)
    etl_partner(oracle_cursor)
    etl_p1a_act_limits(oracle_cursor)
    etl_pin_prefix(oracle_cursor)
    etl_utenza_rinnovabile(oracle_cursor)
    etl_stato_utente(oracle_cursor)
    logging.info("ETL Catalogo Done")


def etl_promozioni(oracle_cursor):
    promozioni_data = read_data.read(oracle_cursor, 'sql/promozioni/read_promozioni.sql')
    criteri_data = read_data.read(oracle_cursor, 'sql/promozioni/read_criteri.sql')
    relazioni_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazioni.sql')
    relazioni_b_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazionib.sql')
    relazioni_bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazioni_bonus.sql')
    bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_bonus.sql')
    selezioni_bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_selezione_bonus.sql')
    promozioni_write.new_write(promozioni=promozioni_data, criteri=criteri_data, relazioni=relazioni_data,
                               relazioni_b=relazioni_b_data, bonuss=bonus_data,
                               relazioni_bonus=relazioni_bonus_data,
                               selezioni_bonus=selezioni_bonus_data)


# def etl_promozioni(oracle_cursor, mongo_client):
#     logging.info("Reading and writing 'promozioni'...")
#     promozioni_data = read_data.read(oracle_cursor, 'sql/promozioni/read_promozioni.sql')
#     promozioni_to_insert = pd.read_json(StringIO(promozioni_data))
#     promozioni_write.write(promozioni_to_insert, mongo_client)
#     # relazioni e criteri
#     logging.info("Reading and writing 'relazioni' and 'criteri'. Updating previous promozioni document...")
#     relazioni_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazioni.sql')
#     relazioni_to_insert = pd.read_json(StringIO(relazioni_data))
#     criteri_data = read_data.read(oracle_cursor, 'sql/promozioni/read_criteri.sql')
#     criteri_to_insert = pd.read_json(StringIO(criteri_data))
#     relazioni_write.write(relazioni_to_insert=relazioni_to_insert, criteri_to_insert=criteri_to_insert,
#                           mongo_client=mongo_client)
#     logging.info("Done Reading and writing 'relazioni' and 'criteri'.")
#     # relazione bonus document
#     logging.info(
#         "Reading and writing 'bonus', 'relazioni_b', 'relazioni_bonus'. Updating previous 'promozioni' document...")
#     relazioni_b_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazionib.sql')
#     relazioni_bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_relazioni_bonus.sql')
#     relazioni_bonus_to_insert = pd.read_json(StringIO(relazioni_bonus_data))
#     relazioni_b_to_insert = pd.read_json(StringIO(relazioni_b_data))
#     bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_bonus.sql')
#     bonus_to_insert = pd.read_json(StringIO(bonus_data))
#     relazioni_bonus_write.write(relazioni_bonus_to_insert, relazioni_b_to_insert, bonus_to_insert, mongo_client)
#     logging.info("Done Reading and writing 'bonus', 'relazioni_b', 'relazioni_bonus'.")
#     # bonus in and
#     logging.info("writing 'bonusNotrel', Updating previous 'promozioni' document...")
#     bonus_write.write(bonus_to_insert, mongo_client)
#     logging.info("Done writing 'bonusNotrel'.")
#     # criteri in and
#     logging.info("writing 'criteri', Updating previous 'promozioni' document...")
#     criteri_write.write(criteri_to_insert, mongo_client)
#     logging.info("Done writing 'criteri'.")
#     logging.info("Reading and Writing 'selezioni_bonus' and 'bonus', Updating previous 'promozioni' document...")
#     selezioni_bonus_data = read_data.read(oracle_cursor, 'sql/promozioni/read_selezione_bonus.sql')
#     selezioni_bonus_to_insert = pd.read_json(StringIO(selezioni_bonus_data))
#     selezione_bonus_write.write(selezioni_bonus_to_insert, mongo_client)
#     logging.info("Done Reading and Writing 'selezioni_bonus' and 'bonus'")


def etl_ricarica(oracle_cursor):
    logging.info("Reading and writing 'ricarica'...")
    ricarica_data = read_data.read(oracle_cursor, 'sql/ricarica/read_ricarica.sql')
    ricarica_write.write(ricarica_data)
    logging.info("Done Reading and writing 'ricarica'.")


def etl_campagna_sec(oracle_cursor):
    logging.info("Reading and writing 'campagna_sec'...")
    campagna_sec_data = read_data.read(oracle_cursor, 'sql/catalogo/campagna_sec_read.sql')
    campagna_sec_write.write(campagna_sec_data)
    logging.info("Done Reading and writing 'campagna_sec'.")


def etl_punto_vendita(oracle_cursor):
    logging.info("Reading and writing 'punto_vendita'...")
    punto_vendita_data = read_data.read(oracle_cursor, 'sql/catalogo/punto_vendita_read.sql')
    punto_vendita_write.write(punto_vendita_data)
    logging.info("Done Reading and writing 'punto_vendita'.")


def etl_white_list(oracle_cursor):
    logging.info("Reading and writing 'white_list'...")
    white_list_data = read_data.read(oracle_cursor, 'sql/catalogo/white_list_read.sql')
    white_list_utenti_data = read_data.read(oracle_cursor, 'sql/catalogo/white_list_utenti_read.sql')
    white_list_write.write(white_list_data, white_list_utenti_data)
    logging.info("Done Reading and writing 'white_list'.")


def etl_black_list(oracle_cursor):
    logging.info("Reading and writing 'black_list'...")
    black_list_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_read.sql')
    black_list_utenti_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_utente_read.sql')
    black_list_multipla_ass_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_multipla_ass.sql')
    black_list_write.write(black_list_data, black_list_utenti_data, black_list_multipla_ass_data)
    black_list_multipla_data = read_data.read(oracle_cursor, 'sql/catalogo/black_list_multipla_read.sql')
    black_list_write.write_multipla(black_list_multipla_data)
    logging.info("Done Reading and writing 'black_list'.")


def etl_anagrafiche_auth_id(oracle_cursor):
    logging.info("Reading and writing 'anagrafiche_auth'...")
    anagrafiche_data = read_data.read(oracle_cursor, 'sql/catalogo/anagrafiche_id_auth_read.sql')
    anagrafiche_write.write(anagrafiche_data)
    logging.info("Done Reading and writing 'anagrafiche_auth'.")


def etl_partner(oracle_cursor):
    logging.info("Reading and writing 'partner'...")
    partner_data = read_data.read(oracle_cursor, 'sql/catalogo/partner_read.sql')
    partner_write.write(partner_data)
    logging.info("Done Reading and writing 'partner'.")


def etl_p1a_act_limits(oracle_cursor):
    logging.info("Reading and writing 'etl_p1a_act_limits'...")
    limits_data = read_data.read(oracle_cursor, 'sql/catalogo/p1a_act_limits_read.sql')
    limits_write.write(limits_data)
    logging.info("Done Reading and writing 'etl_p1a_act_limits'.")


def etl_pin_prefix(oracle_cursor):
    logging.info("Reading and writing 'pin_prefix'...")
    pin_prefix_data = read_data.read(oracle_cursor, 'sql/catalogo/pin_prefix_read.sql')
    pin_prefix_write.write(pin_prefix_data)
    logging.info("Done Reading and writing 'pin_prefix'.")


def etl_utenza_rinnovabile(oracle_cursor):
    logging.info("Reading and writing 'utenza_rinnovabile'...")
    utenza_rinnovabile_data = read_data.read(oracle_cursor, 'sql/catalogo/utenza_rinnovabile_read.sql')
    utenza_rinnovabile_write.write(utenza_rinnovabile_data)
    logging.info("Done Reading and writing 'utenza_rinnovabile'.")


def etl_stato_utente(oracle_cursor):
    logging.info("Reading and writing 'stato_utente'...")
    stato_utente_data = read_data.read(oracle_cursor, 'sql/catalogo/stato_utente_read.sql')
    stato_utente_write.write(stato_utente_data)
    logging.info("Done Reading and writing 'stato_utente'.")


def etl_iniziativa(oracle_cursor):
    logging.info("Reading and writing 'iniziativa'...")
    iniziativa_data = read_data.read(oracle_cursor, 'sql/iniziativa/read_iniziativa.sql')

    write_data.write_file(iniziativa_data, "iniziativa.json")
    logging.info("Done Reading and writing 'iniziativa'.")
    logging.info("Reading and writing 'p1a_mapping'...")
    p1a_mapping = read_data.read(oracle_cursor, 'sql/iniziativa/read_p1a_mapping.sql')
    write_data.write_file(p1a_mapping, "p1a_mapping.json")
    logging.info("Done Reading and writing 'p1a_mapping'.")
    logging.info("Reading and writing 'p1a_storico_pin'...")
    p1a_storico_pin_data = read_data.read(oracle_cursor, 'sql/iniziativa/read_p1a_storico_pin.sql')
    write_data.write_file(p1a_storico_pin_data, "p1a_storico_pin.json")
    logging.info("Done Reading and writing 'p1a_storico_pin'.")
    logging.info("Reading and writing 'pin_iniziativa'...")
    pin_iniziativa = read_data.read(oracle_cursor, 'sql/iniziativa/read_pin_iniziativa.sql')
    write_data.write_file(pin_iniziativa, "pin_iniziativa.json")
    logging.info("Done Reading and writing 'pin_iniziativa'.")
    logging.info("Reading and writing 'storico_pin'...")
    storico_pin = read_data.read(oracle_cursor, 'sql/iniziativa/read_storico_pin.sql')
    write_data.write_file(storico_pin, "storico_pin.json")
    logging.info("Done Reading and writing 'storico_pin'.")
