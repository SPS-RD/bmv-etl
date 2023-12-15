select IDSTORICOPIN as storico_pin_id,
       PIN,
       ASSEGNATARIO,
       UTILIZZATORE,
       STATO,
       CAUSALE,
       CANALEASSEGNAMENTO as canale_assegnamento,
       CANALEUTILIZZO as canale_utilizzo,
       DATATRANSAZIONE as data_transazione,
       DATAMODIFICA as data_modifica,
       CANALESERVIZIO as canale_servizio,
       TIPOSERVIZIO as tipo_servizio,
       NUMBRUCIATUREPINUTENTE as num_bruciature_pin_utente
from PV_STORICO_PIN_INIZIATIVA
