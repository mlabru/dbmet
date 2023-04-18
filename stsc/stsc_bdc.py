# -*- coding: utf-8 -*-
"""
stsc_send_bdc

2022.apr  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging

# postgres
import psycopg2

# local
import stsc.stsc_defs as df

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def bdc_connect(fs_user=df.DS_USER, fs_pass=df.DS_PASS, fs_host=df.DS_HOST, fs_db=df.DS_DB):
    """
    connect to BDC
    """
    # logger
    M_LOG.info(">> bdc_connect")

    # create connection
    l_bdc = psycopg2.connect(host=fs_host, database=fs_db, user=fs_user, password=fs_pass)
    assert l_bdc is not None

    # return
    return l_bdc

# ---------------------------------------------------------------------------------------------
def bdc_save_stsc(fdt_gmt, fs_lat, fs_lng, f_bdc):
    """
    write stsc data to BDC

    :param fdt_gmt (datetime): date GMT
    :param fs_lat (str): latitude
    :param fs_lng (str): longitude
    :param f_bdc (conn): connection to BDC
    """
    # logger
    M_LOG.info(">> bdc_save_stsc")

    # build date & time from stsc date
    ldt_date = fdt_gmt.date()

    # datetime to timestamp
    ls_tsp = fdt_gmt.strftime("%Y%m%d %H:%M:%S")

    # make query
    ls_query = f"insert into stsc_n(latitude, longitude, datahoraocorrencia, " \
               f"ano, mes, dia) values ({fs_lat}, {fs_lng}, '{ls_tsp}', " \
               f"{ldt_date.year}, {ldt_date.month}, {ldt_date.day})"

    # write to BDC
    bdc_write(f_bdc, ls_query)

# ---------------------------------------------------------------------------------------------
def bdc_write(f_bdc, fs_query):
    """
    execute query on BDC
    """
    # logger
    M_LOG.info(">> bdc_write")

    # create cursor
    l_cursor = f_bdc.cursor()
    assert l_cursor

    # execute query
    l_cursor.execute(fs_query)

    # commit
    f_bdc.commit()

# < the end >----------------------------------------------------------------------------------
