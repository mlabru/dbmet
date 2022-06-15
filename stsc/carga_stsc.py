# -*- coding: utf-8 -*-
"""
carga_stsc

2022.apr  1.0  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import argparse
import datetime
import logging
import sys

# local
import stsc.stsc_data_redemet as dr
import stsc.stsc_defs as df
import stsc.stsc_send_bdc as sb
import utils.utl_dates as dt

# < defines >----------------------------------------------------------------------------------

# time range
DI_DELTA_TIME = 1

# input date format
DS_DATE_FORMAT = "%Y-%m-%dT%H"

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def arg_parse():
    """
    parse command line arguments
    arguments parse: <initial date> <final date>

    :returns: arguments
    """
    # logger
    M_LOG.info(">> arg_parse")

    # create parser
    l_parser = argparse.ArgumentParser(description="STSC (Tempo Severo).")
    assert l_parser

    # args
    l_parser.add_argument("-f", "--dfnl", dest="dfnl", action="store", default="x",
                          help="Final date.")
    l_parser.add_argument("-i", "--dini", dest="dini", action="store", default="x",
                          help="Initial date.")

    # return arguments
    return l_parser.parse_args()

# ---------------------------------------------------------------------------------------------
def trata_stsc(fdt_ini: datetime.datetime, f_bdc):
    """
    trata stsc

    :param fdt_ini (datetime): data de início
    :param f_bdc: conexão com o banco de dados
    """
    # logger
    M_LOG.info(">> trata_stsc")

    # format full date
    ls_date = fdt_ini.strftime("%Y%m%d%H")

    # info
    print(f"Processing date: {ls_date}.")

    # STSC data dictionary
    ldct_stsc = dr.redemet_get_stsc(ls_date)

    if not ldct_stsc:
        # logger
        M_LOG.warning("Error for date: %s", ls_date)
        # return
        return

    # lista de horas
    llst_horas = ldct_stsc["anima"]
    # lista de lat/lng
    llst_lat_lngs = ldct_stsc["stsc"]

    # para todas as horas...
    for li_ndx, ls_hora in enumerate(llst_horas):
        # hora e minutos do anima
        li_hor = int(ls_hora[0:2])
        li_min = int(ls_hora[3:])

        # hora da busca ?
        if fdt_ini.hour != li_hor:
            # despreza
            continue

        # ajusta minutos na data
        fdt_ini = fdt_ini.replace(minute=li_min)

        # pata todos os lat/lng...
        for ldct_ll in llst_lat_lngs[li_ndx]:
            # grava registro no banco
            sb.bdc_save_stsc(fdt_ini, ldct_ll["la"], ldct_ll["lo"], f_bdc)

# ---------------------------------------------------------------------------------------------
def main():
    """
    drive app
    """
    # get program arguments
    l_args = arg_parse()

    # connect BDC
    l_bdc = sb.bdc_connect()
    assert l_bdc

    # time delta
    ldt_1hour = datetime.timedelta(hours=DI_DELTA_TIME)

    # date range
    ldt_ini, li_delta = dt.get_date_range(l_args, DI_DELTA_TIME, DS_DATE_FORMAT)

    # for all dates...
    for _ in range(li_delta):
        # trata_stsc
        trata_stsc(ldt_ini, l_bdc)

        # next hour
        ldt_ini += ldt_1hour

    # close BDC
    l_bdc.close()

# ---------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(level=df.DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxsize)

    # run application
    sys.exit(main())

# < the end >----------------------------------------------------------------------------------
