# -*- coding: utf-8 -*-
"""
opm_db

2022.jun  mlabru  initial version (Linux/Python)
"""
# < imports >-------------------------------------------------------------------

# python library
import datetime
import json
import logging

# requests
import requests

# local
import opmet.opm_defs as df

import utils.utl_dates as ud

# < logging >-------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# -------------------------------------------------------------------------------------
def get_auth_token():
    """
    generate token

    :returns: string token
    """
    # try make request
    try:
        # get auth token
        l_response = requests.request("POST",
                                      df.DS_URL_AUTH,
                                      headers=df.DDCT_HEADER_AUTH,
                                      data=df.DS_PAYLOAD_AUTH,
                                      verify=False)

    # em caso de erro...
    except requests.exceptions.RequestException as l_err:
        # logger
        M_LOG.error("error in auth token request: %s.", str(l_err))

    # ok ?
    if 200 == l_response.status_code:
        # return token
        return l_response.text

    # senão,...
    else:
        # logger
        M_LOG.warning("error %d. No auth token.", l_response.status_code)
        # logger
        M_LOG.warning("request: %s", str(fs_url))

    # returns a mimic token
    return df.DS_DEFAULT_TOKEN

# -------------------------------------------------------------------------------------
def get_data_type(f_args):
    """
    get extraction data types

    :param f_args: received arguments

    :returns: list with data type
    """
    # extraction type
    ls_type = str(f_args.type).lower()

    # default ?
    if "x" == ls_type:
        # return ok
        return df.DLST_DEFAULT_PARAM

    # valid ?
    if ls_type in df.DLST_DEFAULT_PARAM:
        # return list with data type
        return [ls_type]

    # logger
    M_LOG.error("error in data type: %s. Assuming defaults.", str(ls_type))

    # return default
    return df.DLST_DEFAULT_PARAM

# -------------------------------------------------------------------------------------
def get_date_range(f_args):
    """
    get initial and final dates

    :param f_args: received arguments

    :returns: initial date and delta in hours
    """
    # delta
    li_delta = 1

    # no date at all ?
    if ("x" == f_args.dini) and ("x" == f_args.dfnl):
        # days before
        li_days = int(f_args.days) + 1
        if li_days not in [1, 31]:
            # adjust days
            li_days = 1

        # get 1/31 day before
        ldt_day_minus = datetime.datetime.now() - datetime.timedelta(days=li_days)

        # build initial date
        ldt_ini = ldt_day_minus.replace(minute=0, second=0)

    # just initial date ?
    elif ("x" != f_args.dini) and ("x" == f_args.dfnl):
        # parse initial date
        ldt_ini = ud.parse_date(f_args.dini, DS_DATE_FORMAT)

    # just final date ?
    elif ("x" == f_args.dini) and ("x" != f_args.dfnl):
        # parse final date
        ldt_fnl = ud.parse_date(f_args.dfnl, DS_DATE_FORMAT)

        # delta
        ldt_ini = ldt_fnl - datetime.timedelta(minutes=59)

    # so, both dates
    else:
        # parse initial date
        ldt_ini = ud.parse_date(f_args.dini, DS_DATE_FORMAT)

        # parse final date
        ldt_fnl = ud.parse_date(f_args.dfnl, DS_DATE_FORMAT)

        # calculate difference
        li_delta = ldt_fnl - ldt_ini
        li_delta = int(li_delta.total_seconds() / 3600)

    # return initial date and delta in hours
    return ldt_ini, li_delta

# -------------------------------------------------------------------------------------
def get_ext_stations(fdct_header: dict) -> str:
    """
    get extra stations (besides FAB)

    :returns: comma separated list of extra stations
    """
    # request payload
    ldct_payload: dict = {}

    # stations list
    llst_xtra = []

    # try make request
    try:
        # make request locations
        l_response = requests.request("GET", df.DS_URL_LOCT,
                                      headers=fdct_header,
                                      data=ldct_payload,
                                      verify=False)
        # ok ?
        if 200 == l_response.status_code:
            # response data
            ldct_data = json.loads(l_response.text)

            # stations list
            llst_data = ldct_data.get("locationCodes", [])

            # clean up list
            llst_xtra = [lxt for lxt in llst_data if lxt[:2] not in ["EE", "SB", "TE", "XX"]]

        # senão, ok...
        else:
            # logger
            M_LOG.warning("error %d. Location codes not found.", l_response.status_code)
            # logger
            M_LOG.warning("request: %s", str(fs_url))

    # em caso de erro...
    except requests.exceptions.RequestException as l_err:
        # logger
        M_LOG.error("error in location codes request: %s", str(l_err))

    # return list as string
    return ",".join(llst_xtra)

# < the end >--------------------------------------------------------------------------
