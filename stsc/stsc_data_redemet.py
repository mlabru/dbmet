# -*- coding: utf-8 -*-
"""
stsc_data_redemet

2022.apr  mlabru   initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import json
import logging

# requests
import requests

# local
import stsc.stsc_defs as df

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def redemet_get_stsc(fs_date):
    """
    request de dados de tempo severo

    :param fs_date (str): date to search

    :returns: STSC data if found else None
    """
    # request de dados de tempo severo
    l_response = requests.get(df.DS_REDEMET_URL.format(df.DS_REDEMET_KEY, fs_date))

    # ok ?
    if 200 != l_response.status_code:
        # logger
        M_LOG.error("REDEMET STSC data not found. Code: %s", str(l_response.status_code))
        # return error
        return None

    try:
        # decode REDEMET STSC data
        ldct_stsc = json.loads(l_response.text)

    # em caso de erro...
    except json.decoder.JSONDecodeError as l_err:
        # logger
        M_LOG.error("REDEMET STSC data decoding error for %s: %s.", fs_date, str(l_err))
        # return error
        return None

    # flag status
    lv_status = ldct_stsc.get("status", None)

    if lv_status is None or not lv_status:
        # logger
        M_LOG.error("REDEMET STSC data status error for %s: %s", fs_date, str(ldct_stsc))
        # return error
        return None

    # STSC data
    ldct_data = ldct_stsc.get("data", None)

    if not ldct_data:
        # logger
        M_LOG.error("REDEMET STSC data have no data field for %s: %s", fs_date, str(ldct_stsc))
        # return error
        return None

    # trata
    return {"anima": ldct_data["anima"],
            "stsc": ldct_data["stsc"]}

# < the end >----------------------------------------------------------------------------------
