# -*- coding: utf-8 -*-
"""
rad_defs

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

# REDEME API Key
DS_REDEMET_KEY = os.getenv("DS_REDEMET_KEY")

# < defines >----------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.DEBUG

# diferença da hora local para GMT (valor absoluto)
DI_DIFF_GMT = 3

# REDEMET
DS_REDEMET_URL = ("https://api-redemet.decea.mil.br/produtos/radar/{1}?"
                  "api_key={0}&area={2}{3}")

# < the end >----------------------------------------------------------------------------------
