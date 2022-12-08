# -*- coding: utf-8 -*-
"""
carga_inmet

2021.may  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import datetime
import json
import logging
import sys

# requests
import requests

# local
import inmet.inm_dw as db
import inmet.inm_defs as df

# < defines >----------------------------------------------------------------------------------

# apitempo do INMet
DS_INMET_URL = "https://apitempo.inmet.gov.br/"
# api das estações automáticas
DS_INMET_STT = DS_INMET_URL + "estacoes/T"
# api de dados horários
DS_INMET_DAT = DS_INMET_URL + "estacao/dados/{}/{}"

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def get_data_hora() -> tuple[str, str]:
    """
    returns data & hora formatadas

    :returns: data & hora
    """
    # datetime object containing current date and time, but 3 hours ahead (UTC)
    ldt_now_utc = datetime.datetime.now() + datetime.timedelta(hours=df.DI_DIFF_UTC)

    # format date
    ls_data = ldt_now_utc.strftime("%Y-%m-%d")

    # format time
    ls_hora = ldt_now_utc.strftime("%H") + "00"

    # returns data & hora
    return ls_data, ls_hora

# ---------------------------------------------------------------------------------------------
def get_stations_altitudes() -> dict:
    """
    dicionário de altitudes de estações automáticas

    :returns: dicionário de altitudes
    """
    # init dicionário de altitudes
    ldct_stations_alt = {}

    # request de dados de estações tomáticas
    l_response = requests.get(DS_INMET_STT)

    # not ok ?
    if 200 != l_response.status_code:
        # logger
        M_LOG.fatal("%s não está acessível (1). Aborting.", DS_INMET_URL)
        # abort
        sys.exit(-1)

    # lista de estações tomáticas
    llst_stations = json.loads(l_response.text)

    # for all stations...
    for ldct_station in llst_stations:
        # código da estação
        ls_station = str(ldct_station["CD_ESTACAO"])

        # station altitude or 0
        ldct_stations_alt[ls_station] = float(ldct_station.get("VL_ALTITUDE", 0))

    # return dicionário de altitudes
    return ldct_stations_alt

# ---------------------------------------------------------------------------------------------
def main():
    """
    main
    """
    # connect DW
    l_dw = db.connect_dw()
    assert l_dw

    # stations altitudes
    ldct_stations_alt = get_stations_altitudes()
    M_LOG.debug("ldct_stations_alt: %s", str(ldct_stations_alt))

    # data & hora formatadas
    ls_data, ls_hora = get_data_hora()

    # recupera dados horários referentes a todas as estações automáticas do dia e hora
    l_response = requests.get(DS_INMET_DAT.format(ls_data, ls_hora))
    M_LOG.debug("url: %s", str(l_response.url))

    # not ok ?
    if 200 != l_response.status_code:
        # logger
        M_LOG.fatal("%s não está acessível (2). Aborting.", DS_INMET_URL)
        # abort
        sys.exit(-1)

    # lista de dados de estações automáticas
    llst_dados = json.loads(l_response.text)

    # para todas as estações automáticas...
    for ldct_dado in llst_dados:
        # código da estação
        ls_station = str(ldct_dado["CD_ESTACAO"])
        M_LOG.debug("código da estação: %s", ls_station)

        # manual station ?
        if ls_station not in ldct_stations_alt:
            # logger
            M_LOG.warning("estação %s não é automática. Skipping.", ls_station)
            # next
            continue

        # invalid entry ?
        if ldct_dado["VEN_DIR"] is None and \
           ldct_dado["PRE_INS"] is None and \
           ldct_dado["CHUVA"] is None:
            # logger
            M_LOG.warning("registro vazio em %s. Skipping.", ls_station)
            # next
            continue

        # write data to DW
        db.send_to_dw(l_dw, ldct_dado, ldct_stations_alt)

    # close DW
    l_dw.close()

# ---------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(level=df.DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxsize)

    # run application
    main()

    # exit ok
    sys.exit(0)

# < the end >----------------------------------------------------------------------------------
