SELECT
    IDCRITERIO as criterio_id,
    IDPROMOZIONE as promozione_id,
    IDRILEVATORE as rilevatore_id,
    IDCOLLETTORE as collettore_id,
    NOMECRITERIO as nome_criterio,
    DESCRIZIONE as descrizione,
    NOTE as note,
    ORIGINE as origine,
    NUMEROEVENTI as numero_eventi,
    TIPO as tipo,
    PRIORITA as priorita,
    BASENOMEFILE as base_nome_file,
    AUTOREULTIMAMODIFICA as autore_ultima_modifica,
    DATAULTIMAMODIFICA as data_ultima_modifica,
    TU_VERSIONE as tu_versione,
    TU_SERVIZIO as tu_servizio,
    TU_CANALE as tu_canale,
    TU_OPERAZIONE as tu_operazione,
    TU_CATEGORIA as tu_categoria,
    TU_SOTTOCATEGORIA as tu_sotto_categoria,
    TU_ESITOOPERAZIONE as tu_esito_operazione,
    TU_NODO as tu_nodo,
    TU_OGGETTO as tu_oggetto,
    TU_MESSAGGIO as tu_messaggio,
    TU_TIPOOGGETTO as tu_tipo_oggetto,
    CODICETUC as codice_tuc,
    LOGICACRITERIO as logica_criterio,
    SB_CODICE as sb_codice,
    MSGDICONFERMA as msg_di_conferma,
    NOMECAMPAGNASMSC as nome_campagna_smsc,
    FORMATOMSISDN as formato_msisdn,
    NOMECRITERIOCOMM as nome_criterio_comm,
    DPPS_TIPOEVENTO as dpps_tipo_evento,
    DPPS_CODICESERVIZIO as dpps_codice_servizio,
    DPPS_OFFERTA as dpps_offerta,
    DPPS_OFFERTAOLD as dpps_offerta_old,
    DPPS_SUPPLIER as dpps_supplier,
    DPPS_VOLTAGE as dpps_voltage,
    DPPS_MEMORY as dpps_memory,
    DPPS_FEATURE as dpps_feature,
    PIN_TIPOOPERATORE as pin_tipo_operatore,
    RIC_CANALE as ric_canale,
    RIC_IMPORTO as ric_importo,
    RIC_OPLOGICO as ric_oplogico,
    CODICETUPA as codice_tupa,
    TU_MATCH_MSG as tu_match_msg,
    TU_CONTENTPROVIDER as tu_content_provider,
    RIC_IMPORTO_MAX as ric_importo_max,
    RIC_CODICESERVIZIO as ric_codice_servizio,
    RIC_CODICEABI as ric_codice_abi,
    UTILIZZOSEC as utilizzo_sec,
    OADC as oadc,
    MSGCORTESIASUCC as msg_corteria_succ,
    MSGCORTESIAERR as msg_cortesia_err,
    OADC_MO as oadc_mo,
    EVENTOUNICO as evento_unico,
    TU_TESTOLIBERO1 as tu_testo_libero1,
    TU_TESTOLIBERO2 as tu_testo_libero2,
    TU_TESTOLIBERO3 as tu_testo_libero3,
    TU_TESTOLIBERO4 as tu_testo_libero4,
    TU_TESTOLIBERO5 as tu_testo_libero5,
    TU_TESTOLIBERO6 as tu_testo_libero6,
    TU_TESTOLIBERO7 as tu_testo_libero7,
    TU_TESTOLIBERO8 as tu_testo_libero8,
    TU_TESTOLIBERO9 as tu_testo_libero9,
    TU_TESTOLIBERO10 as tu_testo_libero10,
    TU_NUMERICOLIBERO1 as tu_numerico_libero1,
    TU_NUMERICOLIBERO2 as tu_numerico_libero2,
    TU_NUMERICOLIBERO3 as tu_numerico_libero3,
    TU_NUMERICOLIBERO4 as tu_numerico_libero4,
    TU_NUMERICOLIBERO5 as tu_numerico_libero5,
    TU_NUMERICOLIBERO6 as tu_numerico_libero6,
    TU_NUMERICOLIBERO7 as tu_numerico_libero7,
    TU_NUMERICOLIBERO8 as tu_numerico_libero8,
    TU_NUMERICOLIBERO9 as tu_numerico_libero9,
    TU_NUMERICOLIBERO10 as tu_numerico_libero10,
    DATARICHIESTAMNP as data_richiesta_mnp,
    DATAFINERICHIESTAMNP as data_fine_richiesta_mnp,
    CODICEGESTOREPROVENIENZA as codice_gestore_provenienza,
    SDR_CODICESERVIZIO as sdr_codice_servizio,
    NUMEROMASSIMOCRITERI as numero_massimo_criteri,
    NUMEROMINIMOGIORNI as numero_minimo_giorni,
    DPPS_SERVIZIORIESUMATO as dpps_servizio_riesumato,
    FLAGWHITELIST as flag_white_list,
    FLAGUTENTECOOP as flag_utente_coop,
    IDWHITELIST as white_list_id,
    PUA_TIPOLOGIASEGNALATORE as pua_tipologia_segnalatore,
    NUMEROMAXGGACCUMULO as numero_max_gg_accumulo,
    GISP_SERVIZIO as gisp_servizio,
    GISP_STATO as gisp_stato,
    MNP_TIPOLOGIA as mnp_tipologia,
    FLAGDDT as flag_ddt,
    FLAGESCLUSIONETOC as flag_esclusione_toc,
    TIPOACQUISIZIONE as tipo_acquisizione,
    WLACQUISIZIONE as wlAcquisizione,
    BLACQUISIZIONE as bl_acquisizione,
    FLAGBLACKLISTCRITERIO as flag_black_list_criterio,
    IDBLACKLISTCRITERIO as id_black_list_criterio,
    MSC_TIPOOPERAZIONE as msc_tipo_operazione,
    MSC_TIPOVARIAZIONE as msc_tipo_variazione,
    MSC_CODICESERVIZIO as ms_codice_servizio,
    MSC_CAUSALECESSAZIONE as msc_causale_cessazione,
    MD_CODICEOFFERTA as md_codice_offerta,
    MD_OFFERTAMULTIDEVICE as md_offerta_multi_device,
    MD_CAUSALECESSAZIONE as md_causale_cessazione,
    FLAGMT as flag_mt,
    TESTOMT as testo_mt,
    VASGK_ACTION as vasgk_action,
    VASGK_ID_CSP as vasgk_id_csp,
    VASGK_TYPE as vasgk_type,
    VASGK_DESCR as vasgk_descr,
    GS_CODICEOFFERTAPP as gs_codice_offerta_pp,
    GS_TIPOOPERAZIONE as gs_tipo_operazione,
    GS_OADC as gs_oadc,
    SMSMO_MONITORAGGIO as sms_mo_monitoraggio,
    SMSMO_INCALL as sms_mo_in_call,
    SMSMO_NOMEOFFERTA as sms_mo_nome_offerta,
    SMSMO_MT_MONITORAGGIO_KO as sms_mo_mt_monitoraggio_ko,
    GINO_CODICE_OFFERTA as gino_codice_offerta,
    GINO_SUBSYS as gino_subsys,
    GINO_TIPO_OPERAZIONE as gino_tipo_operazione,
    SEL_MONITORAGGIO as sel_monitoraggio,
    SEL_MT_MONITORAGGIO as sel_mt_monitoraggio,
    SEL_GESTIONE_NR as sel_gestione_nr,
    SEL_BONUS_NR as sel_bonus_nr,
    GFP_FLAG_IMP as gfp_flag_imp,
    BPM_TIPO_OPERAZIONE as bpm_tipo_operazione,
    BPM_CATEGORY as bpm_category,
    SMSMO_INCALL_MONIT as sms_mo_in_call_monit,
    GFP_TIPOOPERAZIONE as gfp_tipo_operazione,
    GFP_OFFERTAATTERRAGGIO as gfp_offerta_atterraggio,
    VERIFICA_CF as verifica_cf,
    OPERATORE as operatore,
    VERIFICA_CF_DA as verifica_cf_da,
    VERIFICA_CF_A as verifica_cf_a,
    GINO_TIPOLOGIA_CONTROLLO as gino_tipologia_controllo,
    GINO_TOC as gino_toc,
    GINO_TIPO_CONTROLLO_SUBSYS,
    CHECKCAMBIONUMERO as check_cambio_numero,
    WL_TIPOLOGIA_CONTROLLO as wl_tipologia_controllo,
    FLAGIVR as flag_ivr,
    IVR_CARTA_SERVIZI as ivr_carta_servizi,
    FLAGBLACKLISTMULTIPLA as flag_black_list_multipla,
    IDBLACKLISTMULTIPLA as id_black_list_multipla,
    FLAGPROMORUSH as flag_promo_rush,
    FLAG_RIC_MODE as flag_ric_mode,
    OPSC_TIPO_OPERAZIONE as opsc_tipo_operazione,
    OPSC_CARTA_SERVIZI as opsc_carta_servizi,
    OPSC_FLAG_INVIO_SMS as opsc_flag_invio_sms,
    OPSC_TESTO_SMS as opsc_testo_sms,
    OPSC_LA as opsc_la
FROM
    PV_CRITERIO