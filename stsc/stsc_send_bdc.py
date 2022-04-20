# -*- coding: utf-8 -*-
"""
stsc_send_bdc

2022.apr  1.0  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging
import os

# postgres
import psycopg2

# dotenv
from dotenv import load_dotenv

# local
import stsc_defs as df

# < environment >------------------------------------------------------------------------------

# take environment variables from .env
load_dotenv()

# DB connection
DS_HOST = os.getenv("DS_HOST")
DS_USER = os.getenv("DS_USER")
DS_PASS = os.getenv("DS_PASS")
DS_DB = os.getenv("DS_DB")

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def bdc_connect(fs_user=DS_USER, fs_pass=DS_PASS, fs_host=DS_HOST, fs_db=DS_DB):
    """
    connect to BDC
    """
    # create connection
    l_bdc = psycopg2.connect(host=fs_host, database=fs_db, user=fs_user, password=fs_pass)
    assert l_bdc

    # return
    return l_bdc

# ---------------------------------------------------------------------------------------------
def bdc_save_stsc(fdt_gmt, fo_stsc, f_bdc):
    """
    write stsc data to BDC
           
    :param fdt_gmt (datetime): date GMT
    :param fo_stsc (SMETAR): carrapato METAF
    :param f_bdc (conn): connection to BDC
    """
    # stsc date
    # ls_day = fo_stsc.s_forecast_time[:2]
    # ls_hour = fo_stsc.s_forecast_time[2:4]
    # ls_min = fo_stsc.s_forecast_time[4:6]

    # build date & time
    ldt_date = fdt_gmt.date()
    ldt_time = fdt_gmt.time()
    """
    id serial4 NOT NULL,
    latitude float8 NULL,
    longitude float8 NULL,
    datahoraocorrencia timestamp NULL,
    ano int4 NULL,
    mes int2 NULL,
    dia int2 NULL,
    id_stsc bigserial NOT NULL
    """
    # make query
    ls_query = "insert into stsc_n(id, latitude, longitude, datahoraocorrencia, " \
               "ano, mes, dia, id_stsc) values ({}, {}, {}, {}, {}, {}, {}, {}) " \
               .format(fo_stsc.s_icao_code, ldt_date, ldt_time,
               li_vis, li_vis, li_vis, li_vis, li_vis)

    # write to BDC
    bdc_write(f_bdc, ls_query)

# ---------------------------------------------------------------------------------------------
def bdc_write(f_bdc, fs_query):
    """
    execute query on BDC
    """
    # create cursor
    l_cursor = f_bdc.cursor()
    assert l_cursor

    # execute query
    l_cursor.execute(fs_query)

    # commit
    f_bdc.commit()

# < the end >----------------------------------------------------------------------------------
