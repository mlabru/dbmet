# -*- coding: utf-8 -*-
"""
opm_defs

2022.jun  mlabru  initial version (Linux/Python)
"""
# < imports >--------------------------------------------------------------------------

# python library
import logging
import os

# dotenv
import dotenv

# < environment >----------------------------------------------------------------------

# take environment variables from .env
dotenv.load_dotenv()

# mongo db
DS_DB_ADDR = os.getenv("DS_MONGO_ADDR")
DI_DB_PORT = int(os.getenv("DI_MONGO_PORT"))

# payload for token
DS_PAYLOAD_AUTH = os.getenv("DS_PAYLOAD_AUTH")

# < defines >--------------------------------------------------------------------------

# logging level
DI_LOG_LEVEL = logging.WARNING

# retry each 2 minutes
DI_RETRY = 2

# site DECEA
DS_SITE = "https://opmet.decea.mil.br/"

# URL for token
DS_URL_AUTH = DS_SITE + "adm/login"

# request header for token
DDCT_HEADER_AUTH = {
    "Content-Type": "text/plain"
}

# request default token
DS_DEFAULT_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJvc3dhbGRvam9sZiIsImF1dGgiOlt7ImF1dGhvcml0eSI6ImF1ZGl0LmMifSx7ImF1dGhvcml0eSI6ImF1ZGl0LmQifSx7ImF1dGhvcml0eSI6ImF1ZGl0LnIifSx7ImF1dGhvcml0eSI6ImF1ZGl0LnUifSx7ImF1dGhvcml0eSI6ImJkYy1zZXJ2aWNlLnJlYWQifSx7ImF1dGhvcml0eSI6ImNoYW5nZS5wYXNzd29yZCJ9XSwicHJvZmlsZVJvbGUiOiJTWVNURU0iLCJpYXQiOjE2MTcxOTExMDQsImV4cCI6MTYxODA1NTEwNH0.VDcw4LBxhsTKV1EMgY5qTYVbN30mLrHlFjQvFgcI9GU"

# param list
DS_IEPV = "iepv"
DLST_PARAM = [DS_IEPV, "ptu", "wind"]
DLST_DEFAULT_PARAM = [DS_IEPV]

# URL
DS_URL_DATE = DS_SITE + "bdc/search{}/observationdate?begindate={}&enddate={}"
DS_URL_ICAO = DS_SITE + "bdc/search{}/observationdate?begindate={}&enddate={}&icaocodes={}"
DS_URL_LOCT = DS_SITE + "bdc/locations"

# < the end >--------------------------------------------------------------------------
