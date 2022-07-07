# -*- coding: utf-8 -*-
"""
amd_defs

2022.apr  mlabru  initial version (Linux/Python)
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

# DB connection
DS_HOST = os.getenv("DS_HOST")
DS_USER = os.getenv("DS_USER")
DS_PASS = os.getenv("DS_PASS")
DS_DB = os.getenv("DS_DB")

# REDEME API Key
DS_REDEMET_KEY = os.getenv("DS_REDEMET_KEY")

# < defines >----------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# diferença da hora local para GMT (valor absoluto)
DI_DIFF_GMT = 3

# REDEMET
DS_REDEMET_URL = ("https://api-redemet.decea.mil.br/produtos/amdar"
                  "?api_key={0}&data_ini={1}&data={2}")

# < the end >----------------------------------------------------------------------------------
