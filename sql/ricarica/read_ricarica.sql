select msisdn, CODICE_OFFERTA, DATARICARICA as data_ricarica, IDPROMOZIONE as promozione_id
from pv_ivr_ricarica
where dataricarica > sysdate -180