select ID                as p1a_storico_pin_id,
       PIN,
       CFUTILIZZATORE    as cf_utilizzatore,
       CFPROMOTORE       as cf_promotore,
       IDUTILIZZATORE    as utilizzatore_id,
       IDPROMOTORE       as promotore_id,
       CODICE_INIZIATIVA as codice_iniziativa,
       STATO,
       DATATRANSAZIONE   as data_transazione,
       DATAMODIFICA      as data_modifica
from PV_P1A_STORICO_PIN