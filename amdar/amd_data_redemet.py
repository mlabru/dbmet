# -*- coding: utf-8 -*-
"""
amd_data_redemet

2022.apr  mlabru   initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import json
import logging
import typing

# requests
import requests

# local
import amdar.amd_defs as df

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def redemet_get_amdar(fs_date_ini: str, fs_date_fin: str) -> typing.Any:
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
    l_response = requests.get(df.DS_REDEMET_URL.format(df.DS_REDEMET_KEY, fs_date_ini, fs_date_fin))

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
