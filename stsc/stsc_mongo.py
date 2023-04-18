# -*- coding: utf-8 -*-
"""
stsc_mongo

2023.apr  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging

# mongoDB
import pymongo

# local
import stsc.stsc_defs as df  

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
def save_data(f_mongo_client, fs_date: str, flst_stsc: list):
    """
    save data

    :param f_mongo_client: MongoDB client connection
    :param fs_date (str): date
    :param flst_stsc (list): data to be saved 
    """
    # logger
    M_LOG.info(">> save_data")

    # check input
    assert f_mongo_client is not None

    # have data ?
    if not flst_stsc:
        # logger
        M_LOG.warning("empty list. Nothing write to DB.")
        # return
        return

    # banco de dados
    l_banco_dados_stsc = f_mongo_client["stsc"]
    assert l_banco_dados_stsc is not None

    # collection
    l_collection = l_banco_dados_stsc["stsc"]
    assert l_collection is not None

    try:
        # insert dictionary
        l_collection.insert_one({"datahora": fs_date, "eventos": flst_stsc})

    # em caso de erro,...
    except Exception as l_err:
        # logger
        M_LOG.error("could not insert colletion on MongoDB: %s.", str(l_err))

# < the end >----------------------------------------------------------------------------------
