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

# DB connection
DS_DW_HOST = os.getenv("DS_DW_HOST")
DS_DW_USER = os.getenv("DS_DW_USER")
DS_DW_PASS = os.getenv("DS_DW_PASS")
DS_DW_DB = os.getenv("DS_DW_DB")

# < defines >----------------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# UTC is ahead of us
DI_DIFF_UTC = 3

# < the end >----------------------------------------------------------------------------------
