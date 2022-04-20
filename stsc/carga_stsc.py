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
import os
import sys

# local
import stsc_defs as df
import stsc_send_bdc as sb
import stsc_data_redemet as dr

import utl_dates as dt
 
# < defines >----------------------------------------------------------------------------------

# time range
DI_DELTA_TIME = 1

# input date format
DS_DATE_FORMAT = "%Y-%m-%dT%H"

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ----------------------------------------------------------------------------------------------
def arg_parse():
    """
    parse command line arguments
    arguments parse: <initial date> <final date>

    :returns: arguments
    """
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

# ----------------------------------------------------------------------------------------------
def trata_stsc(fdt_ini: datetime.datetime, f_bdc):
    """
    trata stsc

    :param fdt_ini (datetime): data de início
    :param f_bdc: conexão com o banco de dados
    """
    # logger
    M_LOG.info(">> trata_stsc")
    
    # format full date
    ls_date: str = fdt_ini.strftime("%Y%m%d%H")

    # show info
    print(f"Processing date: {ls_date}.")

    # get STSC data
    ldct_stsc: dict = dr.redemet_get_stsc(ls_date)

    # lista de horas
    llst_anima: list = ldct_stsc["anima"]
    # lista de lat/lng
    llst_stsc: list = ldct_stsc["stsc"]

    # para todas as horas...
    for li_ndx, ls_hora in enumerate(llst_anima):
        # minutos e segundos da hora
        li_min = int(ls_hora[0:2])
        li_seg = int(ls_hora[3:])
        # ajusta minutos e segundos na data
        fdt_ini = fdt_ini.replace(minute=li_min, second=li_seg)        

        # pata todos os lat/lng...
        for ldct_ll in llst_stsc[li_ndx]:
            # grava registro no banco
            sb.bdc_save_stsc(fdt_ini, ldct_ll["la"], ldct_ll["lo"], f_bdc)
    
# ----------------------------------------------------------------------------------------------
def main():
    """
    main
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
    for li_i in range(1):  #li_delta):
        # create thread trata_carrapato
        trata_stsc(ldt_ini, l_bdc)
    
        # save new initial
        ldt_ini += ldt_1hour

    # close BDC
    l_bdc.close()

# ----------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(level=df.DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxsize)

    # run application
    sys.exit(main())
        
# < the end >-----------------------------------------------------------------------------------