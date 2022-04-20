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
        # datetime object containing current date and time, but 3 hours ahead (GMT)
        ldt_ini = datetime.datetime.now() + datetime.timedelta(hours=df.DI_DIFF_GMT)
        # build initial date
        ldt_ini = ldt_ini

    # just initial date ?
    elif ("x" != f_args.dini) and ("x" == f_args.dfnl):
        # parse initial date
        ldt_ini = parse_date(f_args.dini)

        # datetime object containing current date and time, but 3 hours ahead (GMT)
        ldt_fnl = datetime.datetime.now() + datetime.timedelta(hours=df.DI_DIFF_GMT)
        # build initial date
        ldt_fnl = ldt_fnl.replace(minute=0)

        # calculate difference in hours
        li_delta = ldt_fnl - ldt_ini
        li_delta = int(li_delta.total_seconds() / 3600)

    # just final date ?
    elif ("x" == f_args.dini) and ("x" != f_args.dfnl):
        # parse final date
        ldt_fnl = parse_date(f_args.dfnl)

        # delta
        ldt_ini = ldt_fnl - datetime.timedelta(hours=1)

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
    return ldt_ini.replace(minute=0, second=0, microsecond=0), li_delta

# ---------------------------------------------------------------------------------------------
def parse_date(fs_data):
    """
    parse date
    
    :param fs_data: date to be parsed

    :returns: date in datetime format
    """
    try:
        # parse data
        ldt_date = datetime.datetime.strptime(fs_data, "%Y-%m-%dT%H")
        # build initial date
        ldt_date = ldt_date.replace(minute=0)

    # em caso de erro,...
    except Exception as l_err:
        # logger
        M_LOG.error("Date format error: %s. Aborting.", l_err)

        # abort
        sys.exit(-1)

    # return date in datetime format
    return ldt_date

# ----------------------------------------------------------------------------------------------
def trata_stsc(ldt_ini, l_bdc):
    """
    trata stsc
    """
    # logger
    M_LOG.info(">> trata_stsc")
    
    # format full date
    ls_date = ldt_ini.strftime("%Y%m%d%H")

    # show info
    print(f"Processando data: {ls_date}.")

    # get STSC data
    ldct_stsc = dr.redemet_get_stsc(ls_date)

    # lista de horas
    llst_anima = ldct_stsc["anima"]
    # lista de lat/lng
    llst_stsc = ldct_stsc["stsc"]

    # para todas as horas...
    for li_ndx, ls_hora in enumerate(llst_anima):
        # pata todos os lat/lng...
        for ldct_ll in llst_stsc[li_ndx]:
            print("ls_hora:", ls_hora, " / ", "ldct_ll:", str(ldct_ll))
    
# ----------------------------------------------------------------------------------------------
def main():
    """
    main
    """
    # get program arguments
    l_args = arg_parse()

    # connect BDC
    l_bdc = None  # sb.bdc_connect()
    # assert l_bdc
    
    # time delta
    ldt_1hour = datetime.timedelta(hours=1)
    
    # date range
    ldt_ini, li_delta = get_date_range(l_args)
    M_LOG.debug("ldt_ini: %s", str(ldt_ini))
    M_LOG.debug("li_delta: %s", str(li_delta))
    
    # for all dates...
    for li_i in range(1):  #li_delta):
        # create thread trata_carrapato
        trata_stsc(ldt_ini, l_bdc)
    
        # save new initial
        ldt_ini += ldt_1hour

    # close BDC
    # l_bdc.close()

# ----------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(level=df.DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxint)

    # run application
    sys.exit(main())
        
# < the end >-----------------------------------------------------------------------------------
