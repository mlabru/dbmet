# -*- coding: utf-8 -*-
"""
inm_dw

2021.may  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging
import math
import typing

# postgres
import psycopg2

# local
import inmet.inm_defs as df

# < defines >----------------------------------------------------------------------------------

# ft -> m
DF_FT2M = 0.3048
# m/s -> kt
DF_MS2KT = 1.943844492

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def connect_dw(fs_user: typing.Optional[str] = df.DS_DW_USER,
               fs_pass: typing.Optional[str] = df.DS_DW_PASS,
               fs_host: typing.Optional[str] = df.DS_DW_HOST,
               fs_db: typing.Optional[str] = df.DS_DW_DB):
    """
    connect to DW

    :param fs_user (str): DW user
    :param fs_pass (str): password
    :param fs_host (str): DW host
    :param fs_db (str): DW database

    :returns: DW connections
    """
    # create connection
    l_dw = psycopg2.connect(host=fs_host, database=fs_db, user=fs_user, password=fs_pass)
    assert l_dw

    # return DW connection
    return l_dw

# ---------------------------------------------------------------------------------------------
def send_to_dw(f_dw, fdct_dado: dict, fdct_alt: dict):
    """
    write data to DW

    :param f_dw: conexão com o DW
    :param fdct_dado (dict): dicionário de dados
    :param fdct_alt (dict): dicionário de altitudes das estações
    """
    # código da estação
    ls_station = str(fdct_dado["CD_ESTACAO"])

    # altitude
    lf_alt = float(fdct_alt.get(ls_station, 0))
    M_LOG.debug("lf_alt: %s", str(lf_alt))

    # QFE
    lf_qfe = float(fdct_dado.get("PRE_INS", 0))
    # QNH
    lf_qnh = lf_qfe * math.exp(5.2561 * math.log(288 / (288 - 0.0065 * (lf_alt * DF_FT2M))))

    # create cursor
    l_cursor = f_dw.cursor()
    assert l_cursor

    # make query
    # pylint: disable=duplicate-string-formatting-argument, consider-using-f-string
    ls_query = "insert into dado_meteorologico_inmet(ven_dir, dt_medicao,"\
               "dc_nome, chuva, pre_ins, vl_latitude, pre_min, umd_max,"\
               "pre_max, ven_vel, uf, pto_min, tem_max, rad_glo, pto_ins,"\
               "ven_raj, tem_ins, umd_ins, cd_estacao, tem_min, vl_longitude,"\
               "hr_medicao, umd_min, pto_max, ven_vel_kt, ven_raj_kt, qfe_hpa,"\
               "qnh_hpa) values ({}, '{}', '{}', {}, {}, {}, {}, {}, {}, {},"\
               "'{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, {}, '{}', {},"\
               "{}, {}, {}, {}, {})".format(
                   int(fdct_dado.get("VEN_DIR", 0)),
                   str(fdct_dado["DT_MEDICAO"]),
                   str(fdct_dado["DC_NOME"]),
                   float(fdct_dado.get("CHUVA", 0)),
                   lf_qfe,
                   float(fdct_dado.get("VL_LATITUDE", 0)),
                   float(fdct_dado.get("PRE_MIN", 0)),
                   int(fdct_dado.get("UMD_MAX", 0)),
                   float(fdct_dado.get("PRE_MAX", 0)),
                   float(fdct_dado.get("VEN_VEL", 0)),
                   str(fdct_dado["UF"]),
                   float(fdct_dado.get("PTO_MIN", 0)),
                   float(fdct_dado.get("TEM_MAX", 0)),
                   float(fdct_dado.get("RAD_GLO", 0)),
                   float(fdct_dado.get("PTO_INS", 0)),
                   float(fdct_dado.get("VEN_RAJ", 0)),
                   float(fdct_dado.get("TEM_INS", 0)),
                   int(fdct_dado.get("UMD_INS", 0)),
                   ls_station,
                   float(fdct_dado.get("TEM_MIN", 0)),
                   float(fdct_dado.get("VL_LONGITUDE", 0)),
                   str(fdct_dado["HR_MEDICAO"]),
                   int(fdct_dado.get("UMD_MIN", 0)),
                   float(fdct_dado.get("PTO_MAX", 0)),
                   float(fdct_dado.get("VEN_VEL", 0)) * DF_MS2KT,
                   float(fdct_dado.get("VEN_RAJ", 0)) * DF_MS2KT,
                   lf_qfe,
                   lf_qnh)

    # execute query
    l_cursor.execute(ls_query)

    # commit
    f_dw.commit()

# < the end >----------------------------------------------------------------------------------
