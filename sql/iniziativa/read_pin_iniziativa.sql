select PIN,
       PV_PIN_INIZIATIVA.IDINIZIATIVA as iniziativa_id,
       SERIALE,
       PV_PIN_INIZIATIVA.NOMEINIZIATIVA as nome_iniziativa,
       NOMEPARTNER as nome_partner,
       ASSEGNATARIO,
       UTILIZZATORE,
       STATO,
       CAUSALE,
       CANALEASSEGNAMENTO as canale_assegnamento,
       CANALEUTILIZZO as canale_utilizzo,
       DATATRANSAZIONE as data_transazione,
       DATAMODIFICA as data_modifica,
       FLAGEVENTORILEVATO as flag_evento_rilevato,
       DATAEVENTORILEVATO as data_evento_rilevato,
       CANALESERVIZIO as canale_servizio,
       TIPOSERVIZIO as tipo_servizio,
       FLAGINVIONOTIFICA as flag_invio_notifica,
       NUMBRUCIATUREPINUTENTE as num_bruciature_pin_utente,
       FLAGWHITELIST as flag_white_list,
       FLAGUTENTECOOP as flag_utente_coop,
       TIPOLOGIAUTILIZZATORE as tipologia_utilizzatore,
       IDWHITELIST as white_list_id,
       FLAGPORTAUNAMICO as flag_porta_un_amico

from PV_PIN_INIZIATIVA
         inner join PV_INIZIATIVA on PV_PIN_INIZIATIVA.IDINIZIATIVA = PV_INIZIATIVA.IDINIZIATIVA
where PV_INIZIATIVA.STATOINIZIATIVA = 'Attiva'