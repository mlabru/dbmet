# -*- coding: utf-8 -*-
"""
inm_defs

2021.may  mlabru  initial version (Linux/Python)
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

# < defines >----------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# UTC is ahead of us
DI_DIFF_UTC = 3

# DB connection
DS_HOST = os.getenv("DS_HOST")
DS_USER = os.getenv("DS_USER")
DS_PASS = os.getenv("DS_PASS")
DS_DB = os.getenv("DS_DB")

# < the end >----------------------------------------------------------------------------------
