# -*- coding: utf-8 -*-
"""
radar_image

2024.may  mlabru  initial version (Linux/Python)
"""
# < imports >--------------------------------------------------------------------------

# python library
import argparse
import datetime
import logging
import sys

# local
import radar.rad_data_redemet as dr
import radar.rad_defs as df
import utils.utl_dates as dt

# < defines >--------------------------------------------------------------------------

# time range
DI_DELTA_TIME = 1

# input date format
DS_DATE_FORMAT = "%Y-%m-%dT%H"

# < logging >--------------------------------------------------------------------------

# logger
M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# -------------------------------------------------------------------------------------
def arg_parse():
    """
    parse command line arguments
    arguments parse: <initial date:now> <hours:1> <echo:05km> <area:sr>

    :returns: arguments
    """
    # logger
    M_LOG.info(">> arg_parse")

    # create parser
    l_parser = argparse.ArgumentParser(description="Download radar images.")
    assert l_parser

    # args
    l_parser.add_argument("-i", "--dini", dest="dini", action="store", default="x",
                          help="Initial date.")
    l_parser.add_argument("-n", "--hours", dest="hours", action="store", default="1",
                          help="Hours.")
    l_parser.add_argument("-e", "--echo", dest="echo", action="store", default="05km",
                          help="Echo type.")
    l_parser.add_argument("-r", "--radar", dest="area", action="store", default="sr",
                          help="Radar.")

    # return arguments
    return l_parser.parse_args()

# -------------------------------------------------------------------------------------
def trata_radar(f_args, fdt_ini: datetime.datetime):
    """
    trata radar

    :param fdt_ini (datetime): data de início
    """
    # logger
    M_LOG.info(">> trata_radar")

    # format full initial date
    ls_date = fdt_ini.strftime("%Y%m%d%H")

    # show info
    print(f"Processing date: {ls_date} tipo: {f_args.echo} area: {f_args.area}.")

    # get radar data
    llst_radar = dr.redemet_get_radar(f_args.echo, ls_date, f_args.area)
    M_LOG.debug("llst_radar: %s", str(llst_radar))

    if not llst_radar:
        # logger
        M_LOG.warning("Error for date: %s", ls_date)
        # return
        return

    # para todas as imagens
    for ldct_img in llst_radar:
        # download da imagem
        # save_image(fdt_ini, ldct_voo)
        print("download da imagem")

# -------------------------------------------------------------------------------------
def main():
    """
    main
    """
    # get program arguments
    l_args = arg_parse()

    # no date at all ?
    if ("x" == l_args.dini):
        # datetime object containing current date and time, but 3 hours ahead (GMT)
        ldt_ini = datetime.datetime.now() + datetime.timedelta(hours=DI_DIFF_GMT)

        # hours range
        li_range = 1

    # just initial date ?
    elif ("x" != l_args.dini):
        # parse initial date
        ldt_ini = dt.parse_date(l_args.dini, DS_DATE_FORMAT)

        # hours range
        li_range = int(l_args.hours) if 0 < int(l_args.hours) < 25 else 1

    # time delta for processing
    ldt_1hour = datetime.timedelta(hours=DI_DELTA_TIME)

    # for all times...
    for _ in range(li_range):
        # create thread trata_radar
        trata_radar(l_args, ldt_ini)

        # save new initial
        ldt_ini += ldt_1hour

# -------------------------------------------------------------------------------------
# this is the bootstrap process
#
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

# < the end >--------------------------------------------------------------------------
