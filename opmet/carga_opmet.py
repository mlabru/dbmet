# -*- coding: utf-8 -*-
"""
carga_opmet

2022.jun  mlabru   tabela de localidades
2021.may  oswaldo  location
2021.apr  oswaldo  data extraction type selector, empty lists, counter window
2021.apr  oswaldo  initial & final dates
2021.apr  oswaldo  generate authorization token
2021.apr  oswaldo  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import argparse
import datetime
import json
import logging
import sys
import time

# requests
import requests

# local
import opmet.opm_db as db
import opmet.opm_defs as df
import utils.utl_dates as ud

# < defines >----------------------------------------------------------------------------------

# date format
DS_DATE_FORMAT = "%Y-%m-%dT%H:%M"

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def arg_parse():
    """
    parse command line arguments
    arguments parse: -i <initial date> -f <final date> -c <ICAO code> -t <data type>

    :returns: parsed arguments
    """
    # create parser
    l_parser = argparse.ArgumentParser(description="Carga OpMet > MongoDB.")
    assert l_parser

    # args
    l_parser.add_argument("-c", "--code", dest="code", action="store", default="x",
                          help="ICAO code.")
    l_parser.add_argument("-i", "--dini", dest="dini", action="store", default="x",
                          help="Initial date.")
    l_parser.add_argument("-f", "--dfnl", dest="dfnl", action="store", default="x",
                          help="Final date.")
    l_parser.add_argument("-t", "--type", dest="type", action="store", default="x",
                          help="Data type.")

    # return arguments
    return l_parser.parse_args()

# ---------------------------------------------------------------------------------------------
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
        M_LOG.error("error in request: %s.", str(l_err))

    # any error ?
    if 200 != l_response.status_code:
        # logger
        M_LOG.warning("error on auth token.")

        # returns a mimic token
        return df.DS_DEFAULT_TOKEN

    # return token
    return l_response.text

# ---------------------------------------------------------------------------------------------
def get_data_type(f_args):
    """
    get extraction data types

    :param f_args: received arguments

    :returns: list with data type
    """
    # extraction type
    ls_type = str(f_args.type).lower()

    # valid ?
    if ls_type in df.DLST_PARAM:
        # return list with data type
        return [ls_type]

    # default ?
    if "x" == ls_type:
        # return ok
        return df.DLST_PARAM

    # logger
    M_LOG.error("error in data type: %s. Assuming defaults.", str(ls_type))

    # return default
    return df.DLST_DEFAULT_PARAM

# ---------------------------------------------------------------------------------------------
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
        # get 1 day before
        ldt_day_minus_1 = datetime.datetime.now() - datetime.timedelta(days=1)

        # build initial date
        ldt_ini = ldt_day_minus_1.replace(minute=0)

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

# ---------------------------------------------------------------------------------------------
def get_ext_stations(fdct_header: dict) -> str:
    """
    get extra stations (besides FAB)

    :returns: comma separated list of extra stations
    """
    # build URL
    ls_url = df.DS_URL_LOC

    # request payload
    ldct_payload: dict = {}

    # make request
    l_response = requests.request("GET", ls_url,
                                  headers=fdct_header,
                                  data=ldct_payload,
                                  verify=False)

    # response data
    ldct_data = json.loads(l_response.text)

    # stations list
    llst_data = ldct_data["locationCodes"]

    # clean up list
    llst_data = [lext for lext in llst_data if lext[:2] not in ["EE", "SB", "TE", "XX"]]

    # return
    return ",".join(llst_data)

# ---------------------------------------------------------------------------------------------
def load_param(fdct_header: dict, fs_url: str, fs_param: str):
    """
    get param from OpMet and save to mongoDB

    :param fdct_header (dict): request header
    :param fs_url (str): request URL
    :param fs_param (str): parameter to search and save
    """
    # request payload
    ldct_payload: dict = {}

    # init counter
    li_counter = 0

    # keep trying for while...
    while li_counter < (60 / df.DI_RETRY):
        # try make request
        try:
            # make request
            l_response = requests.request("GET", fs_url,
                                          headers=fdct_header,
                                          data=ldct_payload,
                                          verify=False)
        # em caso de erro...
        except requests.exceptions.RequestException as l_err:
            # logger
            M_LOG.error("error in request: %s", str(l_err))

        # not found ?
        if 404 == l_response.status_code:
            # logger
            M_LOG.warning("data not found.")

        # ok ?
        elif 200 == l_response.status_code:
            # load data
            llst_data = json.loads(l_response.text)

            # logger
            M_LOG.warning("ok. Sending to DB...")

            # save data to mongoDB
            db.save_data(fs_param, llst_data["bdc"])

            # logger
            M_LOG.warning("número de registros carregados: %d", len(llst_data["bdc"]))

            # cai fora
            return

        # senão,...
        else:
            # logger
            M_LOG.warning("an unknow error has occurred in request.")

        # increment counter
        li_counter += 1

        # wait (minutes)
        time.sleep(df.DI_RETRY * 60)

# ---------------------------------------------------------------------------------------------
def trata_param(fdct_header: dict, fs_date_ini: str, fs_date_fnl: str, fs_param: str, f_args):
    """
    build URL for search parameters and load
    """
    # search target ICAO code ?
    if "x" != f_args.code:
        # ICAO code
        ls_code = str(f_args.code).upper()[:4]

        # build URL (target station)
        ls_url = df.DS_URL_OBS.format(fs_param, ls_code, fs_date_ini, fs_date_fnl)

    # senão,...
    else:
        # build URL (all stations)
        ls_url = df.DS_URL_INS.format(fs_param, fs_date_ini, fs_date_fnl)

        # search and save parameter
        load_param(fdct_header, ls_url, fs_param)

        # build URL (extra stations)
        ls_url = df.DS_URL_OBS.format(fs_param, get_ext_stations(fdct_header), fs_date_ini, fs_date_fnl)

    # search and save parameter
    load_param(fdct_header, ls_url, fs_param)

# ---------------------------------------------------------------------------------------------
def main():
    """
    drive app
    """
    # get program arguments
    l_args = arg_parse()

    # create header with auth token
    ldct_header = json.loads(get_auth_token())

    # data type
    llst_type = get_data_type(l_args)

    # date range
    ldt_ini, li_delta = get_date_range(l_args)
    M_LOG.debug("ldt_ini: %s  li_delta: %d", ldt_ini, li_delta)

    # for all dates...
    for _ in range(li_delta):
        # convert initial date
        ls_date_ini = ldt_ini.strftime(DS_DATE_FORMAT)
        M_LOG.debug("ls_date_ini: %s", ls_date_ini)

        # delta
        ldt_fnl = ldt_ini + datetime.timedelta(minutes=59)

        # convert final date
        ls_date_fnl = ldt_fnl.strftime(DS_DATE_FORMAT)
        M_LOG.debug("ls_date_fnl: %s", ls_date_fnl)

        # save new initial
        ldt_ini = ldt_ini + datetime.timedelta(hours=1)

        # for all params...
        for ls_param in llst_type:
            # logger
            M_LOG.warning("running for param: %s.", ls_param)

            # get and load param to mongoDB
            trata_param(ldct_header, ls_date_ini, ls_date_fnl, ls_param, l_args)

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
