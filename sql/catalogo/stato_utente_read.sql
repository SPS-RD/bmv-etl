select idoperazione as operazione_id,
       idpromozione as promozione_id,
       msisdn,
       stato,
       dataultimamodifica as data_ultima_modifica,
       numretry as num_retry,
       utenteesterno as is_utente_esterno,
       tipologiautente as tipologia_utente,
       datafinemonitoraggio as data_fine_monitoraggio,
       datascadenzaofferta as data_scadenza_offerta,
       canaleattivazione as canale_attivazione
from PV_STATOUTENTE