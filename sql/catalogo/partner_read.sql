select CODICEPARTNERPIN          as codice_partner_pin,
       NOMEPARTNER               as nome_partner,
       DESCRIZIONE,
        CAST (ULTIMOCODICEINIZIATIVAPIN as INTEGER) as ultimo_codice_iniziativa_pin

from PV_PARTNER