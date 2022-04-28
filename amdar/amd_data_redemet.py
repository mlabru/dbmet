# -*- coding: utf-8 -*-
"""
amd_data_redemet

2022.apr  1.0  mlabru   initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import json
import logging
import os

# dotenv
from dotenv import load_dotenv

# requests
import requests

# local
import amd_defs as df

# < environment >------------------------------------------------------------------------------

# take environment variables from .env
load_dotenv()

# REDEME API Key
DS_REDEMET_KEY = os.getenv("DS_REDEMET_KEY")

# < defines >----------------------------------------------------------------------------------

# REDEMET
DS_REDEMET_URL = ("https://api-redemet.decea.mil.br/produtos/amdar"
                  "?api_key={0}&data_ini={1}&data={2}")

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def redemet_get_amdar(fs_date_ini: str, fs_date_fin: str) -> list:
    """
    request de dados de informações do AMDAR

    :param fs_date_ini (str): initial date to search
    :param fs_date_fin (str): final date to search

    :returns: AMDAR data if found else None
    """
    # logger
    M_LOG.info(">> redemet_get_amdar")

    # check input
    assert fs_date_ini
    assert fs_date_fin

    # request de dados de informações do AMDAR
    l_response = requests.get(DS_REDEMET_URL.format(DS_REDEMET_KEY, fs_date_ini, fs_date_fin))

    # ok ?
    if 200 != l_response.status_code:
        # logger
        M_LOG.error("AMDAR data not found. Code: %s", str(l_response.status_code))
        # return error
        return None

    try:
        # decode AMDAR data
        ldct_amdar = json.loads(l_response.text)

    # em caso de erro...
    except json.decoder.JSONDecodeError as l_err:
        # logger
        M_LOG.error("AMDAR data decoding error for %s: %s.", fs_date_ini, str(l_err))
        # return error
        return None
        
    # flag status
    lv_status = ldct_amdar.get("status", None)

    if lv_status is None or not lv_status:
        # logger
        M_LOG.error("AMDAR data status error for %s: %s", fs_date_ini, str(ldct_amdar))
        # return error
        return None

    # AMDAR data
    llst_data = ldct_amdar.get("data", None)

    if not llst_data:
        # logger
        M_LOG.error("AMDAR data have no data field for %s: %s", fs_date_ini, str(ldct_amdar))
        # return error
        return None

    # trata
    return llst_data
                                    
# < the end >----------------------------------------------------------------------------------
