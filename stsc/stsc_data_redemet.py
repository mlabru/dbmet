# -*- coding: utf-8 -*-
"""
stsc_data_redemet

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
import stsc_defs as df

# < environment >------------------------------------------------------------------------------

# take environment variables from .env
load_dotenv()

# REDEME API Key
DS_REDEMET_KEY = os.getenv("DS_REDEMET_KEY")

# < defines >----------------------------------------------------------------------------------

# REDEMET
DS_REDEMET_URL = "https://api-redemet.decea.mil.br/produtos/stsc?api_key={0}&data={1}&anima=60"

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
    l_response = requests.get(DS_REDEMET_URL.format(DS_REDEMET_KEY, fs_date))

    # ok ?
    if 200 == l_response.status_code:
        try:
            # decode REDEMET STSC data
            ldct_stsc = json.loads(l_response.text)

        # em caso de erro...
        except json.decoder.JSONDecodeError as l_err:
            # logger
            M_LOG.error("REDEMET STSC data decoding error: %s.", str(l_err))
            # quit
            return None
            
        # flag status
        lv_status = ldct_stsc.get("status", None)

        if lv_status is not None and lv_status:
            # STSC data
            ldct_data = ldct_stsc.get("data", None)

            if ldct_data:
                # trata
                return {"anima": ldct_data["anima"],
                        "stsc": ldct_data["stsc"]}
                                            
            # senão, no ldct_data
            else:
                # logger
                M_LOG.error("REDEMET STSC data have no data field: %s", str(ldct_stsc))

        # senão, no lv_status
        else:
            # logger
            M_LOG.error("REDEMET STSC data status error: %s", str(ldct_stsc))

    # senão,...
    else:
        # logger
        M_LOG.error("REDEMET STSC data not found. Code: %s", str(l_response.status_code))

    # return error
    return None

# < the end >----------------------------------------------------------------------------------
