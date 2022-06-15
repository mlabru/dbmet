# -*- coding: utf-8 -*-
"""
opm_db

2022.jun  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging

# mongoDB
import pymongo

# local
import opmet.opm_defs as df

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def save_data(fs_param: str, flst_data: list):
    """
    save data

    :param fs_param (str): kind
    :param flst_data (list): data to be saved
    """
    # logger
    M_LOG.info(">> save_data")

    # check input
    assert fs_param in df.DLST_PARAM

    # mongoDB connection
    l_conexao_mongo: pymongo.mongo_client.MongoClient
    l_conexao_mongo = pymongo.MongoClient(df.DS_DB_ADDR, df.DI_DB_PORT)
    assert l_conexao_mongo

    # banco de dados Opmet
    l_banco_dados_opmet = l_conexao_mongo.opmet
    assert l_banco_dados_opmet

    # observação meteorologica ?
    if "iepv" == fs_param:
        # observação meteorologica
        l_collection = l_banco_dados_opmet.observacaoMeteorologica
        assert l_collection

    # altitude ?
    elif "ptu" == fs_param:
        # altitude
        l_collection = l_banco_dados_opmet.ptu
        assert l_collection

    # wind ?
    elif "wind" == fs_param:
        # Wind
        l_collection = l_banco_dados_opmet.wind
        assert l_collection

    # senão,...
    else:
        # invalid collection
        l_collection = None

        # logger
        M_LOG.error("invalid collection: %s.", fs_param)

    try:
        # have data ?
        if flst_data:
            # insert list
            l_collection.insert_many(flst_data)

        # senão,...
        else:
            # logger
            M_LOG.warning("empty list.")

    # em caso de timeout...
    except pymongo.errors.ServerSelectionTimeoutError as l_err:
        # logger
        M_LOG.error("timeout on connection to MongoDB: %s.", str(l_err))

    # em caso de erro de conexão...
    except pymongo.errors.ConnectionFailure as l_err:
        # logger
        M_LOG.error("could not connect to MongoDB: %s.", str(l_err))

# < the end >----------------------------------------------------------------------------------
