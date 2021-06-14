# -*- coding: utf-8 -*-
"""
CargaInmet2DB

2021/may  1.0  mlabru   initial version (Linux/Python)
"""
# < imports >--------------------------------------------------------------------------------------

# python library
import datetime
import json
import logging
import math
import sys
import requests

# postgres
import psycopg2

# < defines >--------------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# UTC is ahead of us
DI_DIFF_UTC = 3

# ft -> m
DF_FT2M = 0.3048

# m/s -> kt
DF_MS2KT = 1.943844492

# DB connection
DS_HOST = "172.18.30.30"
DS_USER = "dwclimatologia"
DS_PASS = "dwclimatologia"
DS_DB = "dw_climatologia"

# dicionário de altitudes de estações automáticas
DDCT_STATIONS_ALT = {}

# -------------------------------------------------------------------------------------------------
def connect_bdc(fs_user=DS_USER, fs_pass=DS_PASS, fs_host=DS_HOST, fs_db=DS_DB):
    """
    connect to BDC
    """
    # create connection
    l_bdc = psycopg2.connect(host=fs_host, database=fs_db, user=fs_user, password=fs_pass)
    assert l_bdc

    # return
    return l_bdc

# -------------------------------------------------------------------------------------------------
def get_val(f_dct, fs_key, f_default):
    """
    get value
    """
    # float val
    lval = f_dct.get(fs_key, None)

    # return
    return f_default if lval is None else lval

# -------------------------------------------------------------------------------------------------
def send_to_bdc(f_bdc, fdct_dado, fs_hora):
    """
    write data to BDC
    """
    # altitude
    lf_alt = float(get_val(DDCT_STATIONS_ALT, fdct_dado["CD_ESTACAO"], 0.))
    logging.debug("lf_alt: %s", str(lf_alt))
    
    # QFE
    lf_qfe = float(get_val(fdct_dado, "PRE_INS", 0.))
    # QNH
    lf_qnh = lf_qfe * math.exp(5.2561 * math.log(288 / (288 - 0.0065 * (lf_alt * DF_FT2M))))

    # create cursor
    l_cursor = f_bdc.cursor()
    assert l_cursor

    # make query
    ls_query = "insert into dado_meteorologico_inmet(ven_dir, dt_medicao, dc_nome, chuva, " \
               "pre_ins, vl_latitude, pre_min, umd_max, pre_max, ven_vel, uf, pto_min, " \
               "tem_max, rad_glo, pto_ins, ven_raj, tem_ins, umd_ins, cd_estacao, tem_min, " \
               "vl_longitude, hr_medicao, umd_min, pto_max, ven_vel_kt,ven_raj_kt, qfe_hpa, " \
               "qnh_hpa) values ({}, '{}', '{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, {}, " \
               "{}, {}, {}, {}, {}, '{}', {}, {}, '{}', {}, {}, {}, {}, {}, {})".format(
                   int(get_val(fdct_dado, "VEN_DIR", 0)),
                   str(fdct_dado["DT_MEDICAO"]),
                   str(fdct_dado["DC_NOME"]),
                   float(get_val(fdct_dado, "CHUVA", 0)),
                   lf_qfe,
                   float(get_val(fdct_dado, "VL_LATITUDE", 0)),
                   float(get_val(fdct_dado, "PRE_MIN", 0)),
                   int(get_val(fdct_dado, "UMD_MAX", 0)),
                   float(get_val(fdct_dado, "PRE_MAX", 0)),
                   float(get_val(fdct_dado, "VEN_VEL", 0)),
                   str(fdct_dado["UF"]),
                   float(get_val(fdct_dado, "PTO_MIN", 0)),
                   float(get_val(fdct_dado, "TEM_MAX", 0)),
                   float(get_val(fdct_dado, "RAD_GLO", 0)),
                   float(get_val(fdct_dado, "PTO_INS", 0)),
                   float(get_val(fdct_dado, "VEN_RAJ", 0)),
                   float(get_val(fdct_dado, "TEM_INS", 0)),
                   int(get_val(fdct_dado, "UMD_INS", 0)),
                   str(fdct_dado["CD_ESTACAO"]),
                   float(get_val(fdct_dado, "TEM_MIN", 0)),
                   float(get_val(fdct_dado, "VL_LONGITUDE", 0)),
                   str(fdct_dado["HR_MEDICAO"]),
                   int(get_val(fdct_dado, "UMD_MIN", 0)),
                   float(get_val(fdct_dado, "PTO_MAX", 0)),
                   float(get_val(fdct_dado, "VEN_VEL", 0)) * DF_MS2KT,
                   float(get_val(fdct_dado, "VEN_RAJ", 0)) * DF_MS2KT,
                   lf_qfe,
                   lf_qnh,
                   )

    # execute query
    l_cursor.execute(ls_query)

    # commit
    f_bdc.commit()

# -------------------------------------------------------------------------------------------------
def main():
    """
    main
    """
    # connect BDC
    l_bdc = connect_bdc()
    assert l_bdc

    # datetime object containing current date and time, but 3 hours ahead (UTC)
    ldt_now_utc = datetime.datetime.now() + datetime.timedelta(hours=DI_DIFF_UTC)
    logging.debug("ldt_now_utc: %s", str(ldt_now_utc))

    # format date
    ls_data = ldt_now_utc.strftime("%Y-%m-%d")
    logging.debug("date: %s", ls_data)

    # format time
    ls_hora = ldt_now_utc.strftime("%H") + "00"
    logging.debug("ls_hora: %s", ls_hora)

    # request de dados de estações tomáticas
    response = requests.get("https://apitempo.inmet.gov.br/estacoes/T")

    # ok ?
    if 200 == response.status_code:
        # lista de estações tomáticas
        llst_stations = json.loads(response.text)

        # for all stations...
        for ldct_station in llst_stations:
            # station altitude
            DDCT_STATIONS_ALT[ldct_station["CD_ESTACAO"]] = float(get_val(ldct_station, "VL_ALTITUDE", 0))

    # senão,...
    else:
        # logger
        logging.fatal("apitempo.inmet.gov.br está inacessível (1). Aborting.")

        # quit with error
        sys.exit(255)

    logging.debug("DDCT_STATIONS_ALT: %s", str(DDCT_STATIONS_ALT))

    # recupera dados horários referentes a todas as estações automáticas do dia e hora
    response = requests.get("https://apitempo.inmet.gov.br/estacao/dados/{}/{}".format(ls_data, ls_hora))
    logging.debug("url: %s", str(response.url))

    # ok ?
    if 200 == response.status_code:
        # lista de dados de estações automáticas
        llst_dados = json.loads(response.text)

        # para todas as estações automáticas...
        for ldct_dado in llst_dados:
            # logger
            logging.debug("Código da estação: %s", str(ldct_dado["CD_ESTACAO"]))

            # manual station ?
            if str(ldct_dado["CD_ESTACAO"]) not in DDCT_STATIONS_ALT:
                # logger
                logging.warning("estação %s não é automática. Skipping.", str(ldct_dado["CD_ESTACAO"]))
                # next
                continue

            # invalid entry ?
            if ldct_dado["VEN_DIR"] is None and ldct_dado["PRE_INS"] is None and ldct_dado["CHUVA"] is None:
                # logger
                logging.warning("registro vazio em %s. Skipping.", str(ldct_dado["CD_ESTACAO"]))
                # next
                continue

            # write data to BDC
            send_to_bdc(l_bdc, ldct_dado, ls_hora)

    # senão,...
    else:
        # logger
        logging.fatal("apitempo.inmet.gov.br está inacessível (2). Aborting.")

        # quit with error
        sys.exit(255)

    # close BDC
    l_bdc.close()

# -------------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(level=DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxint)

    # run application
    sys.exit(main())

# < the end >--------------------------------------------------------------------------------------
