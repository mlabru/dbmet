# -*- coding: utf-8 -*-
"""
CargaOpmetToMongoDB

2021/may  1.4  oswaldo  location
2021/apr  1.3  oswaldo  data extraction type selector, empty lists, counter window
2021/apr  1.2  oswaldo  initial & final dates
2021/apr  1.1  oswaldo  generate authorization token
2021/apr  1.0  oswaldo  initial version (Linux/Python)
"""
# < imports >--------------------------------------------------------------------------------------

# python library
import argparse
import datetime
import json
import logging
import requests
import sys
import time

# mongoDB
import pymongo

# < defines >--------------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# retry each 2 minutes
DI_RETRY = 2

# site DECEA
DS_SITE = "https://opmet.decea.mil.br/"

# URL for token
DS_URL_AUTH = DS_SITE + "adm/login"

# payload for token
DS_PAYLOAD_AUTH = "{\"username\":\"oswaldojolf\",\"password\":\"Ice@0pmet\"}"

# request header for token
DDCT_HEADER_AUTH = {
    "Content-Type": "text/plain"
}

# param list
DLST_PARAM = ["iepv", "ptu", "wind"]

# URL
DS_URL_INS = DS_SITE + "bdc/search{}/observationdate?begindate={}&enddate={}"
DS_URL_OBS = DS_SITE + "bdc/search{}/observationdate?icaocodes={}&begindate={}&enddate={}"

DS_DB_ADDR = "172.18.30.30"
DI_DB_PORT = 27017

# < module data >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(DI_LOG_LEVEL)

# -------------------------------------------------------------------------------------------------
def arg_parse():
    """
    parse command line arguments
    arguments parse: <initial date> <final date> <ICAO code> <data type>

    :returns: arguments
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
    #l_parser.add_argument("file_in_1",
    #                      help="First file to sync.")
    #l_parser.add_argument("file_in_2",
    #                      help="Second file to sync.")

    # return arguments
    return l_parser.parse_args()

# -------------------------------------------------------------------------------------------------
def get_auth_token():
    """
    generate token

    :returns: string token
    """
    # try make request
    try:
        # get auth token
        l_response = requests.request("POST", DS_URL_AUTH, headers=DDCT_HEADER_AUTH, data=DS_PAYLOAD_AUTH, verify=False)

    # em caso de erro...
    except requests.exceptions.RequestException as ls_err:
        # logger
        M_LOG.error("Error in request: %s.", ls_err)

    # any error ?
    if 200 != l_response.status_code:
        # logger
        M_LOG.warning("Error on auth token.")

        # mimic a token
        l_response.text = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvc3dhbGRvam9sZiIsImF1dGgiOlt7ImF1dGhvcml0eSI6ImF1ZGl0LmMifSx7ImF1dGhvcml0eSI6ImF1ZGl0LmQifSx7ImF1dGhvcml0eSI6ImF1ZGl0LnIifSx7ImF1dGhvcml0eSI6ImF1ZGl0LnUifSx7ImF1dGhvcml0eSI6ImJkYy1zZXJ2aWNlLnJlYWQifSx7ImF1dGhvcml0eSI6ImNoYW5nZS5wYXNzd29yZCJ9XSwicHJvZmlsZVJvbGUiOiJTWVNURU0iLCJpYXQiOjE2MTcxOTExMDQsImV4cCI6MTYxODA1NTEwNH0.VDcw4LBxhsTKV1EMgY5qTYVbN30mLrHlFjQvFgcI9GU"

    # return token
    return l_response.text
    
# -------------------------------------------------------------------------------------------------
def get_data_type(f_args):
    """
    get extraction data types

    :param f_args: received arguments

    :returns: list with data type
    """
    # extraction type
    ls_type = str(f_args.type).lower()
    
    # valid ?
    if ls_type in DLST_PARAM:
        # return list with data type
        return [ls_type]

    # default ?
    if "x" == ls_type:
        # return ok
        return DLST_PARAM

    # logger
    M_LOG.error("Error in data type: %s. Assuming defaults.", ls_type)

    # return default
    return ["iepv"]

# -------------------------------------------------------------------------------------------------
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
        ldt_ini = parse_date(f_args.dini)

    # just final date ?
    elif ("x" == f_args.dini) and ("x" != f_args.dfnl):
        # parse final date
        ldt_fnl = parse_date(f_args.dfnl)

        # delta
        ldt_ini = ldt_fnl - datetime.timedelta(minutes=59)

    # so, both dates
    else:
        # parse initial date
        ldt_ini = parse_date(f_args.dini)

        # parse final date
        ldt_fnl = parse_date(f_args.dfnl)

        # calculate difference
        li_delta = ldt_fnl - ldt_ini
        li_delta = int(li_delta.total_seconds() / 3600)

    # return initial date and delta in hours
    return ldt_ini, li_delta

# -------------------------------------------------------------------------------------------------
def load_param(fdct_header, fs_date_ini, fs_date_fnl, fs_param, f_args):
    """
    get param from OpMet and save to mongoDB
    """
    # request payload
    ldct_payload = {}

    # search target ICAO code ?
    if "x" != f_args.code:
        # ICAO code
        ls_code = str(f_args.code).upper()[:4]

        # build URL
        ls_url = DS_URL_OBS.format(fs_param, ls_code, fs_date_ini, fs_date_fnl)

    # senão, by date
    else:
        # build URL
        ls_url = DS_URL_INS.format(fs_param, fs_date_ini, fs_date_fnl)

    # init counter
    li_counter = 0

    # keep trying for while...
    while li_counter < (60 / DI_RETRY):
        # try make request
        try:
            # make request
            l_response = requests.request("GET", ls_url, headers=fdct_header, data=ldct_payload, verify=False)

        # em caso de erro...
        except requests.exceptions.RequestException as ls_err:
            # logger
            M_LOG.error("Error in request: %s", ls_err)

        # ok ?
        if 200 == l_response.status_code:
            # load data
            llst_data = json.loads(l_response.text)

            # logger
            M_LOG.warning("Ok. Sending to DB...")

            # save data to mongoDB
            save_data(fs_param, llst_data["bdc"])
            
            # logger
            print(str(llst_data["bdc"]))
            print("número de registros carregados:", len(llst_data["bdc"]))

            # cai fora
            break

        # not fount ?
        elif 404 == l_response.status_code:
            # logger
            M_LOG.warning("Data not found.")

        # senão,...
        else:
            # logger
            M_LOG.warning("An error has occurred.")

        # increment counter
        li_counter += 1

        # wait (minutes)
        time.sleep(DI_RETRY * 60)

# -------------------------------------------------------------------------------------------------
def parse_date(fs_data):
    """
    parse date

    :param fs_data: date to be parsed

    :returns: date in datetime format
    """
    try:
        # parse data
        l_datetime = datetime.datetime.strptime(fs_data, "%Y-%m-%dT%H:%M")

    # em caso de erro,...
    except Exception as lerr:
        # logger
        M_LOG.error("Date format error: %s.", lerr)

        # abort
        sys.exit(-1) 

    # return date in datetime format
    return l_datetime

# -------------------------------------------------------------------------------------------------
def save_data(fs_param, flst_data):
    """
    save data

    :param fs_param (str): kind
    :param flst_data (list): data to be saved 
    """
    # check input
    assert fs_param in DLST_PARAM


    # mongoDB connection
    l_conexao_mongo = pymongo.MongoClient(DS_DB_ADDR, DI_DB_PORT)
    assert l_conexao_mongo 
    
    # banco de dados Opmet
    l_banco_dados_opmet = l_conexao_mongo.opmet
    assert l_banco_dados_opmet

    # observação meteorologica ?
    if "iepv" == fs_param:
        # observação meteorologica
        l_collection = l_banco_dados_opmet.observacaoMeteorologica
        assert l_collection

    # altitude ? 
    elif "ptu" == fs_param:
        # altitude
        l_collection = l_banco_dados_opmet.ptu
        assert l_collection

    # wind ?
    elif "wind" == fs_param:
        # Wind
        l_collection = l_banco_dados_opmet.wind
        assert l_collection

    # senão,...
    else:
        # invalid collection
        l_collection = None

        # logger
        M_LOG.error("Invalid collection: %s.", fs_param)

    try:
        # have data ?
        if flst_data:
            # insert list
            l_collection.insert_many(flst_data)

        # senão,...
        else:
            # logger
            M_LOG.warning("Empty list.")
        
    # em caso de erro de conexão...
    except pymongo.errors.ConnectionFailure as ls_err:
        # logger
        M_LOG.error("Could not connect to MongoDB: %s.", ls_err)

    # em caso de timeout...
    except pymongo.errors.ServerSelectionTimeoutError as ls_err:
        # logger
        M_LOG.error("Timeout on connection to MongoDB: %s.", ls_err)
    
# -------------------------------------------------------------------------------------------------
def main():
    """
    main
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
    for li_i in range(li_delta):
        # convert initial date
        ls_date_ini = ldt_ini.strftime("%Y-%m-%dT%H:%M")
        M_LOG.debug("ls_date_ini: %s", ls_date_ini)

        # delta
        ldt_fnl = ldt_ini + datetime.timedelta(minutes=59)

        # convert final date
        ls_date_fnl = ldt_fnl.strftime("%Y-%m-%dT%H:%M")
        M_LOG.debug("ls_date_fnl: %s", ls_date_fnl)

        # save new initial
        ldt_ini = ldt_ini + datetime.timedelta(hours=1)

        # for all params...
        for ls_param in llst_type:
            # logger
            M_LOG.warning("Param: %s.", ls_param)

            # get and load param to mongoDB
            load_param(ldct_header, ls_date_ini, ls_date_fnl, ls_param, l_args)

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
    