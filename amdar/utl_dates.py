# -*- coding: utf-8 -*-
"""
utl_dates

2022.apr  1.0  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import datetime
import logging
import sys

# < defines >----------------------------------------------------------------------------------

# diferença da hora local para GMT (valor absoluto)
DI_DIFF_GMT = 3

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(logging.DEBUG)

# ---------------------------------------------------------------------------------------------
def get_date_range(f_args, fi_delta: int, fs_format: str) -> tuple:  # [datetime.datetime, int]:
    """
    get initial and final dates

    :param f_args: received arguments
    :param fi_delta (int): intervalo de tempo
    :param fs_format (str): string de formato da data

    :returns (tuple): initial date and delta in hours
    """
    # delta
    li_delta = fi_delta

    # no date at all ?
    if ("x" == f_args.dini) and ("x" == f_args.dfnl):
        # datetime object containing current date and time, but 3 hours ahead (GMT)
        ldt_ini = datetime.datetime.now() + datetime.timedelta(hours=DI_DIFF_GMT)

    # just initial date ?
    elif ("x" != f_args.dini) and ("x" == f_args.dfnl):
        # parse initial date
        ldt_ini = parse_date(f_args.dini, fs_format)

        # datetime object containing current date and time, but 3 hours ahead (GMT)
        ldt_fnl = datetime.datetime.now() + datetime.timedelta(hours=DI_DIFF_GMT)
        # build final date
        ldt_fnl = ldt_fnl.replace(minute=0)

        # calculate difference in hours
        ldt_delta = ldt_fnl - ldt_ini
        li_delta = int(ldt_delta.total_seconds() / 3600.)

    # just final date ?
    elif ("x" == f_args.dini) and ("x" != f_args.dfnl):
        # parse final date
        ldt_fnl = parse_date(f_args.dfnl, fs_format)

        # delta
        ldt_ini = ldt_fnl - datetime.timedelta(hours=fi_delta)

    # so, both dates
    else:
        # parse initial date
        ldt_ini = parse_date(f_args.dini, fs_format)

        # parse final date
        ldt_fnl = parse_date(f_args.dfnl, fs_format)

        # calculate difference
        ldt_delta = ldt_fnl - ldt_ini
        li_delta = int(ldt_delta.total_seconds() / 3600.)

    # return initial date and delta in hours
    return ldt_ini.replace(minute=0, second=0, microsecond=0), li_delta

# ---------------------------------------------------------------------------------------------
# def parse_date(fs_date: str, fs_format: str="%Y-%m-%dT%H:M") -> datetime.datetime:
def parse_date(fs_date: str, fs_format="%Y-%m-%dT%H:M"):
    """
    parse date
    
    :param fs_date (str): date to be parsed

    :returns (datetime): date in datetime format
    """
    try:
        # parse data
        ldt_date = datetime.datetime.strptime(fs_date, fs_format)
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

# < the end >----------------------------------------------------------------------------------
