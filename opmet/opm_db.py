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
def save_data(fs_param: str, flst_data: list, fv_xtra: bool):
    """
    save data

    :param fs_param (str): kind
    :param flst_data (list): data to be saved 
    """
    # logger
    M_LOG.info(">> save_data")

    # check input
    assert fs_param in df.DLST_PARAM

    # have data ?
    if not flst_data:
        # logger
        M_LOG.warning("empty list. Nothing write to DB.")
        # return
        return

    # mongoDB connection
    l_conexao_mongo = pymongo.MongoClient(df.DS_DB_ADDR, df.DI_DB_PORT)
    assert l_conexao_mongo 
    
    # banco de dados Opmet
    l_banco_dados_opmet = l_conexao_mongo.opmet
    assert l_banco_dados_opmet

    # observação meteorológica ?
    if "iepv" == fs_param:
        # somente estações extras ?
        if fv_xtra:
            # observação meteorologica de estações extras
            l_collection = l_banco_dados_opmet.observacaoMeteorologicaNovas
            assert l_collection

        # senão,...
        else:
            # observação meteorologica de estações FAB
            l_collection = l_banco_dados_opmet.observacaoMeteorologica
            assert l_collection

    # altitude ? 
    elif "ptu" == fs_param:
        # altitude
        l_collection = l_banco_dados_opmet.ptu
        assert l_collection

        # logger
        M_LOG.debug("flst_data (PTU): %s", str(flst_data))

    # wind ?
    elif "wind" == fs_param:
        # Wind
        l_collection = l_banco_dados_opmet.wind
        assert l_collection

        # logger
        M_LOG.debug("flst_data (WIND): %s", str(flst_data))

    # senão,...
    else:
        # invalid collection
        l_collection = None
        # logger
        M_LOG.error("invalid collection: %s.", fs_param)

    try:
        # insert list
        l_collection.insert_many(flst_data)
        
    # em caso de erro de conexão...
    except pymongo.errors.ConnectionFailure as l_err:
        # logger
        M_LOG.error("could not connect to MongoDB: %s.", str(l_err))

    # em caso de timeout...
    except pymongo.errors.ServerSelectionTimeoutError as l_err:
        # logger
        M_LOG.error("timeout on connection to MongoDB: %s.", str(l_err))
    
# < the end >----------------------------------------------------------------------------------
