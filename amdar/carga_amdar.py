# -*- coding: utf-8 -*-
"""
carga_amdar

2022.apr  1.0  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import argparse
import datetime
import logging
import sys

# local
import amdar.amd_data_redemet as dr
import amdar.amd_defs as df
import amdar.amd_send_bdc as sb
import amdar.utl_dates as dt

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
    l_parser = argparse.ArgumentParser(description="Informações AMDAR.")
    assert l_parser

    # args
    l_parser.add_argument("-f", "--dfnl", dest="dfnl", action="store", default="x",
                          help="Final date.")
    l_parser.add_argument("-i", "--dini", dest="dini", action="store", default="x",
                          help="Initial date.")

    # return arguments
    return l_parser.parse_args()

# ---------------------------------------------------------------------------------------------
def trata_amdar(fdt_ini: datetime.datetime, f_bdc):
    """
    trata amdar

    :param fdt_ini (datetime): data de início
    :param f_bdc: conexão com o banco de dados
    """
    # logger
    M_LOG.info(">> trata_amdar")

    # format full initial date
    ls_date_ini = fdt_ini.strftime("%Y%m%d%H")

    # show info
    print(f"Processing date: {ls_date_ini}.")

    # final date
    ldt_fin = fdt_ini + datetime.timedelta(hours=DI_DELTA_TIME)

    # format full final date
    ls_date_fin = ldt_fin.strftime("%Y%m%d%H")

    # get AMDAR data
    llst_amdar = dr.redemet_get_amdar(ls_date_ini, ls_date_fin)

    if not llst_amdar:
        # logger
        M_LOG.warning("Error for date: %s", ls_date_ini)
        # return
        return

    # para todas as aeronaves/voos...
    for ldct_voo in llst_amdar:
        # grava registro no banco
        sb.bdc_save_amdar(fdt_ini, ldct_voo, f_bdc)

# ---------------------------------------------------------------------------------------------
def main():
    """
    main
    """
    # get program arguments
    l_args = arg_parse()

    # connect BDC
    l_bdc = sb.bdc_connect()
    assert l_bdc

    # time delta for processing without final date
    ldt_1hour = datetime.timedelta(hours=DI_DELTA_TIME)

    # date range
    ldt_ini, li_delta = dt.get_date_range(l_args, DI_DELTA_TIME, DS_DATE_FORMAT)

    # for all dates...
    for _ in range(li_delta):
        # create thread trata_amdar
        trata_amdar(ldt_ini, l_bdc)

        # save new initial
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
