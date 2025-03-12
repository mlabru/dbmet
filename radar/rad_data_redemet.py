# -*- coding: utf-8 -*-
"""
rad_data_redemet

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
import radar.rad_defs as df

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def redemet_get_radar(fs_tipo: str, fs_data: str, fs_area: str, fi_anima: int = 1) -> typing.Any:
    """
    request de dados de informações do RADAR

    :param fs_tipo (str): echo type
    :param fs_data (str): date to search
    :param fs_area (str): radar
    :param fs_anima (int): number of radar echoes to animate

    :returns: RADAR data if found else None
    """
    # logger
    M_LOG.info(">> redemet_get_radar")

    # check input
    assert fs_tipo
    assert fs_area

    # build options
    ls_opt = ""

    # data ?
    if fs_data:
        # insert data
        ls_opt += f"&data={fs_data}"

    # anima ?
    if 1 < fi_anima < 16:
        # insert anima
        ls_opt += f"&anima={fi_anima}"

    # buid request
    ls_request = df.DS_REDEMET_URL.format(df.DS_REDEMET_KEY, fs_tipo, fs_area, ls_opt)
    M_LOG.debug("ls_request: %s", ls_request)

    # request de dados de informações do RADAR
    l_response = requests.get(ls_request)
    M_LOG.debug("l_response: %s", str(l_response))

    # ok ?
    if 200 != l_response.status_code:
        # logger
        M_LOG.error("RADAR data not found. Code: %s", str(l_response.status_code))
        # return error
        return None

    try:
        # decode RADAR data
        ldct_radar = json.loads(l_response.text)
        M_LOG.debug("ldct_radar: %s", str(ldct_radar))

    # em caso de erro...
    except json.decoder.JSONDecodeError as l_err:
        # logger
        M_LOG.error("RADAR data decoding error for %s: %s.", fs_date, str(l_err))
        # return error
        return None

    # flag status
    lv_status = ldct_radar.get("status", None)
    M_LOG.debug("lv_status: %s", str(lv_status))

    if lv_status is None or not lv_status:
        # logger
        M_LOG.error("RADAR data status error for %s: %s", fs_date, str(ldct_radar))
        # return error
        return None

    # RADAR data
    llst_data = ldct_radar.get("data", None)
    M_LOG.debug("llst_data: %s", str(llst_data))

    if not llst_data:
        # logger
        M_LOG.error("RADAR data have no data field for %s: %s", fs_date, str(ldct_radar))
        # return error
        return None

    # trata
    return llst_data

# < the end >----------------------------------------------------------------------------------
