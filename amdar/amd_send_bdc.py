# -*- coding: utf-8 -*-
"""
amd_send_bdc

2022.apr  1.0  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging
import os
import typing

# dotenv
import dotenv

# postgres
import psycopg2

# local
import amdar.amd_defs as df

# < environment >------------------------------------------------------------------------------

# take environment variables from .env
dotenv.load_dotenv()

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
def bdc_connect(fs_user: typing.Optional[str] = DS_USER,
                fs_pass: typing.Optional[str] = DS_PASS,
                fs_host: typing.Optional[str] = DS_HOST,
                fs_db: typing.Optional[str] = DS_DB):
    """
    connect to BDC.

    :param fs_user (str): username
    :param fs_pass (str): password
    :param fs_host (str): db server host
    :param fs_db (str): db name

    :returns: db connection
    """
    # logger
    M_LOG.info(">> bdc_connect")

    # create connection
    l_bdc = psycopg2.connect(host=fs_host, database=fs_db, user=fs_user, password=fs_pass)
    assert l_bdc

    # return
    return l_bdc

# ---------------------------------------------------------------------------------------------
def bdc_save_amdar(fdt_ini, fdct_voo: dict, f_bdc):
    """
    write amdar data to BDC.

    :param fdt_ini (datetime): date GMT
    :param fdct_voo (dict): dados do vôo
    :param f_bdc (conn): connection to BDC
    """
    # logger
    M_LOG.info(">> bdc_save_amdar")

    # datetime to timestamp
    ls_tsp = fdt_ini.strftime("%Y%m%d %H:%M:%S")

    # make query
    ls_query = (f"insert into amdar(aeronave, voo, orig_dtg, dh_data, fase_voo, "
                f"lat_grau, lat_min, lat_dir, lat_dec, lon_grau, lon_min, lon_dir, "
                f"lon_dec, altitude, temperatura, dir_vento, vel_vento) values "
                f"('{fdct_voo['aeronave']}', '{fdct_voo['voo']}', '{fdct_voo['data']}', "
                f"'{ls_tsp}', '{fdct_voo['fase_voo']}', '0','0','0', {fdct_voo['lat_dec']}, "
                f"'0','0','0', {fdct_voo['lon_dec']}, {fdct_voo['alt']}, {fdct_voo['temp']}, "
                f"{fdct_voo['dir_vento']}, {fdct_voo['vel_vento']})")

    # write to BDC
    bdc_write(f_bdc, ls_query)

# ---------------------------------------------------------------------------------------------
def bdc_write(f_bdc, fs_query: str):
    """
    execute query on BDC.

    :param f_bdc: db connection
    :param fs_query (str): query
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
