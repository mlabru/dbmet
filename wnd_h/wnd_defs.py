# -*- coding: utf-8 -*-
"""
wnd_defs

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

# lista de localidades
DLST_SITES = ["SBAF", "SBES", "SBGR", "SBGW",
              "SBJD", "SBJR", "SBKP", "SBMT",
              "SBRJ", "SBSC", "SBSJ", "SBSP",
              "SBST", "SBTA", ]

# diretório de dados
DS_DATA_DIR = "./data/SRPV-SP"

# < the end >----------------------------------------------------------------------------------
