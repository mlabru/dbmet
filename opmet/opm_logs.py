# -*- coding: utf-8 -*-
"""
opm_logs

2025.mar  mlabru  initial version (Linux/Python)
"""
# < imports >-------------------------------------------------------------------

# python library
import datetime
import logging
import os

# local
# import opmet.opm_defs as df
import tst_defs as df

# < logging >-------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# -------------------------------------------------------------------------------------
def logger_check(fs_error_fn: str = "error.err"):
    """
    se não houver erro, remover o arquivo de erro

    :param fs_log_fn (str): log file name
    :param fs_error_fn (str): error file name

    :returns: none
    """
    # error doesn't exists ?
    if os.path.exists(fs_error_fn) and 0 == os.path.getsize(fs_error_fn):
        # remove error log
        os.remove(fs_error_fn)
        # logger
        logging.info("No error occurred. The error log has been removed.")

# -------------------------------------------------------------------------------------
def logger_generate_filename(fs_prefix: str = "log", fs_extension: str = ".txt") -> str:
    """
    gera um nome de arquivo contendo a data e hora da execução
    """
    # timestamp
    l_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # return filename
    return f"{fs_prefix}_{l_timestamp}{fs_extension}"

# -------------------------------------------------------------------------------------
def logger_setup(fs_app_fn: str = "app.log", fs_error_fn: str = "error.err"):
    """
    setup logging attributes

    :param fs_app_fn (str): app log file name
    :param fs_error_fn (str): error log file name

    :returns: error logger
    """
    # logger message format
    ls_format = "%(asctime)s - %(levelname)s - %(message)s"

    # logger formatter
    l_formatter = logging.Formatter(ls_format)

    # configuração do logger para mensagens normais
    logging.basicConfig(
        datefmt="%Y/%m/%d %H:%M",
        filename=fs_app_fn,
        level=df.DI_LOG_LEVEL,
        format=ls_format
    )

    # disable logging
    # logging.disable(sys.maxsize)

    # logger error handler
    l_error_handler = logging.FileHandler(fs_error_fn)
    l_error_handler.setLevel(logging.ERROR)
    l_error_handler.setFormatter(l_formatter)

    # logger específico para erros
    l_error_logger = logging.getLogger("error_logger")
    l_error_logger.addHandler(l_error_handler)
    
    # return
    # return logging, l_error_logger
    return l_error_logger

# < the end >--------------------------------------------------------------------------
