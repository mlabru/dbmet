# -*- coding: utf-8 -*-
"""
stsc2mongo

2023.apr  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import argparse
import datetime
import dateutil.tz
import logging
import sys

# local
import stsc.stsc_data_redemet as dr
import stsc.stsc_defs as df
import stsc.stsc_mongo as sb

# < defines >----------------------------------------------------------------------------------

# input date format
DS_DATE_FORMAT = "%Y-%m-%d %H:%M"

# < logging >----------------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def trata_stsc(f_mcli) -> None:
    """
    trata stsc

    :param f_mcli: conexão com o banco de dados
    """
    # logger
    M_LOG.info(">> trata_stsc")

    # check input
    assert f_mcli

    # actual date in UTC
    ldt_utc = datetime.datetime.now(dateutil.tz.tzutc())

    # local time
    ldt_local = ldt_utc.astimezone(dateutil.tz.tzlocal())

    # info
    print("Processing date:", ldt_local)

    # STSC data dictionary
    ldct_stsc = dr.redemet_get_stsc()

    if not ldct_stsc:
        # logger
        M_LOG.warning("Error for date: %s", ls_date)
        # return
        return

    # info
    print("Processing size:", len(ldct_stsc["stsc"][0]))

    # lista de horas
    ls_hora = ldct_stsc["anima"][0]

    # split hour & minute
    llst_hora = ls_hora.split(':')

    # ajust UTC time
    ldt_utc = ldt_utc.replace(hour=int(llst_hora[0]), minute=int(llst_hora[1]))

    # format full date
    ls_date = ldt_utc.strftime(DS_DATE_FORMAT)

    # grava registro no banco
    sb.save_data(f_mcli, ls_date, ldct_stsc["stsc"])

# ---------------------------------------------------------------------------------------------
def main() -> None:
    """
    drive app
    """
    # connect MongoDB
    l_mcli = sb.mongo_connect()
    assert l_mcli

    # salva dados stsc
    trata_stsc(l_mcli)

    # close MongoDB
    l_mcli.close()

# ---------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(datefmt="%Y/%m/%d %H:%M",
                        format="%(asctime)s %(message)s",
                        level=df.DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxsize)

    try:
        # run application
        main()
                          
    # em caso de erro...
    except KeyboardInterrupt:
        # logger
        logging.warning("Interrupted.")
    
    # terminate
    sys.exit(0)

# < the end >----------------------------------------------------------------------------------
