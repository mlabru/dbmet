# -*- coding: utf-8 -*-
"""
wnd_db

2022.jun  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging

# mongoDB
import pymongo

# local
import wnd_h.wnd_defs as df  

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def mongo_connect():
    """
    create MongoDB connection
    """
    # logger
    M_LOG.info(">> mongo_connect")

    try:
        # mongoDB connection
        l_mongo_client = pymongo.MongoClient(df.DS_DB_ADDR, df.DI_DB_PORT)

    # em caso de erro de conexão...
    except pymongo.errors.ConnectionFailure as l_err:
        # logger
        M_LOG.error("could not connect to MongoDB: %s.", str(l_err))
        # return error
        return None

    # em caso de timeout...
    except pymongo.errors.ServerSelectionTimeoutError as l_err:
        # logger
        M_LOG.error("timeout on connection to MongoDB: %s.", str(l_err))
        # return error
        return None

    # return ok
    return l_mongo_client

# ---------------------------------------------------------------------------------------------
def save_data(f_mongo_client, fs_loc: str, fdct_data: dict):
    """
    save data

    :param f_mongo_client: MongoDB client connection
    :param fs_loc (str): kind
    :param fdct_data (dict): data to be saved 
    """
    # logger
    M_LOG.info(">> save_data")

    # check input
    assert fs_loc in df.DLST_SITES

    # have data ?
    if not fdct_data:
        # logger
        M_LOG.warning("empty list. Nothing write to DB.")
        # return
        return

    # banco de dados WIND history
    l_banco_dados_wndhis = f_mongo_client["wndhis"]
    assert l_banco_dados_wndhis

    # collection
    l_collection = l_banco_dados_wndhis[fs_loc]
    assert l_collection

    try:
        # insert dictionary
        l_collection.insert_one(fdct_data)

    # em caso de erro,...
    except Exception as l_err:
        # logger
        M_LOG.error("could not insert colletion on MongoDB: %s.", str(l_err))

# < the end >----------------------------------------------------------------------------------
