select PV_BLACKLIST_MULTIPLA_ASS.IDBLACKLIST as black_list_id ,PV_BLACKLIST_MULTIPLA.IDBLACKLISTMULTIPLA as black_list_multipla_id, PV_BLACKLIST_MULTIPLA.NOMELISTAMULTIPLA as nome_lista_multipla
    from PV_BLACKLIST_MULTIPLA_ASS, PV_BLACKLIST_MULTIPLA
where PV_BLACKLIST_MULTIPLA_ASS.IDBLACKLISTMULTIPLA = PV_BLACKLIST_MULTIPLA.IDBLACKLISTMULTIPLA