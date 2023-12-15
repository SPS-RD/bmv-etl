select PIN,
       CFUTILIZZATORE as cf_utilizzatore,
       CFPROMOTORE as cf_promotore,
       IDUTILIZZATORE as utilizzatore_id,
       IDPROMOTORE as promotore_id,
       CODICE_INIZIATIVA,
       DESCRIZIONE_OFFERTA as descrizione_offerta,
       CS_PIN_STATUS as cs_pin_status
from PV_P1A_MAPPING
