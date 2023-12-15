select IDOPERAZIONE             as operazione_id,
       IDPROMOZIONE             as promozione_id,
       IDCRITERIO               as criterio_id,
       MSISDN,
       DATA                     as data_lista,
       NUMRINNOVI               as numero_rinnovi,
       FILEDIFLUSSO,
       DATACONTROLLOAZZERAMENTO as data_controllo_azzeramento
from PV_UTENZARINNOVABILE