# -*- coding: utf-8 -*-
"""
mfas_defs

2022.jun  mlabru  initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import logging
import os

# dotenv
import dotenv

# < environment >------------------------------------------------------------------------------

# take environment variables from .env
dotenv.load_dotenv()

# < constants >--------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# mongo db
DS_DB_ADDR = os.getenv("DS_MONGO_ADDR")
DI_DB_PORT = int(os.getenv("DI_MONGO_PORT"))

# site DECEA
DS_SITE = "http://10.32.40.53/"
DS_URL_DATE = DS_SITE + "api2/diagnostico/lista?dti={}&dtf={}&local={}"
DS_URL_NOW = DS_SITE + "api2/diagnostico"

# lista de localidades
DLST_SITES = ["SBAF", "SBES", "SBGR", "SBGW",
              "SBJD", "SBJR", "SBKP", "SBMT",
              "SBRJ", "SBSC", "SBSJ", "SBSP",
              "SBST", "SBTA", ]

# diretório de dados
DS_DATA_DIR = "./data/mnd"

# < the end >----------------------------------------------------------------------------------
