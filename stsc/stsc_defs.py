# -*- coding: utf-8 -*-
"""
stsc_defs

2023.apr  mlabru  change to MongoDB
2023.apr  mlabru  change REDEMET URL
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

# BDC connection
# DS_HOST = os.getenv("DS_HOST")
# DS_USER = os.getenv("DS_USER")
# DS_PASS = os.getenv("DS_PASS")
# DS_DB = os.getenv("DS_DB")

# mongoDB
DS_DB_ADDR = os.getenv("DS_MONGO_ADDR")
DI_DB_PORT = int(os.getenv("DI_MONGO_PORT"))

# REDEME API Key
DS_REDEMET_KEY = os.getenv("DS_REDEMET_KEY")

# < defines >----------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# diferença da hora local para GMT (valor absoluto)
DI_DIFF_GMT = 3

# REDEMET
DS_REDEMET_URL = "https://api-redemet.decea.mil.br/produtos/stsc?api_key={0}"
DS_REDEMET_DAN = DS_REDEMET_URL + "&data={1}&anima=60"

# < the end >----------------------------------------------------------------------------------
