# -*- coding: utf-8 -*-
"""
MFAS requests

2022.nov  mlabru   initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import datetime
import json
import logging
import requests
import sys

# local
import mfas.mfas_db as db
import mfas.mfas_defs as df
import utils.utl_dates as ud

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def build_dict(flst_data: list) -> dict:
    """
    build data dictionary from data list
    """
    # logger
    M_LOG.info(">> build_dict")

    # init output
    ldct_out = {}

    # init date
    ldate = "@@@"

    # for all items on list...
    for ldct in flst_data:
        # data e hora  
        ldate = ldct.pop("datahora", None)

        # new key ? 
        if not "vento" in ldct_out:
            # init key list
            ldct_out["vento"] = [] 

        # save on list
        ldct_out["vento"].append(ldct) 
        
    # save date on dict
    ldct_out["datahora"] = ldate
    
    ###    
    ldate = ldate.replace(' ', '_')
    with open(f"mfas/data/test/mfas.{ldate}.txt", 'w') as lfh:
        lfh.write(json.dumps(ldct_out))

    # return dict
    return ldct_out
        
# ---------------------------------------------------------------------------------------------
def check_date(flst_data: list) -> bool:
    """
    check if date is correct
    """
    # logger
    M_LOG.info(">> check_date")

    # now
    ldt_now = datetime.datetime.now() + datetime.timedelta(hours=ud.DI_DIFF_GMT)
    # now
    ls_now = f"{ldt_now:%Y-%m-%d %H:%M:00}"

    # dataset date
    ls_date = flst_data[0].get("datahora", "0000-00-00 00:00:00")

    # return error
    return ls_now == ls_date
    
# ---------------------------------------------------------------------------------------------
def main():
    """
    drive app
    """
    # logger
    M_LOG.info(">> main")
    
    # connect to Mongo DB
    l_mongo_db = db.mongo_connect()
    assert l_mongo_db
    
    # monta a URL
    # ls_url = df.DS_URL_DATE.format("2021-01-08 13:30", "2021-01-08 13:30", 2)
    ls_url = df.DS_URL_NOW

    # try make request
    try:
        # make request
        l_response = requests.request("GET", ls_url)

    # em caso de erro...
    except requests.exceptions.RequestException as l_err:
        # logger
        M_LOG.error("error in data request: %s", str(l_err))
        # logger
        M_LOG.warning("request was: %s", str(ls_url))
        # return
        return

    # em caso de erro...
    except Exception as l_err:
        # logger
        M_LOG.error("exception in data request: %s", str(l_err))

    # ok ?
    if 200 == l_response.status_code:
        # load data
        llst_data = json.loads(l_response.text)

        # date ok ?       
        if check_date(llst_data): 
            # save data to mongoDB
            db.save_data(l_mongo_db, build_dict(llst_data))
            # logger
            M_LOG.warning("número de registros carregados: %d\n", len(llst_data))

        # senão,... 
        else:
            # logger
            M_LOG.error("invalid date (%s). Skipping this round.\n", str(llst_data))

        # cai fora
        return
    
    # forbidden ?
    elif 403 == l_response.status_code:
        # logger
        M_LOG.error("request forbidden for %s.", str(ls_url))
        # cai fora
        return

    # not found ?
    elif 404 == l_response.status_code:
        # logger
        M_LOG.error("data not found.")
        # cai fora
        return

    # senão,...
    else:
        # logger
        M_LOG.error("an unknow error %d has occurred in request.", l_response.status_code)
        # logger
        M_LOG.warning("request was: %s", str(ls_url))

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
