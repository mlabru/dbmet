import logging
#import os
#from datetime import datetime

import tst_defs as df
import opm_logs as lg

l_app_log = None
l_error_log = None

def process_data(data):
    """
    função simulada de processamento de dados.
    """
    # l_app_log.info("iniciando o processamento dos dados...")
    logging.info("iniciando o processamento dos dados...")

    try:
        # simula uma possível divisão por zero
        result = 10 / data
        # l_app_log.info("processamento concluído com sucesso.")
        logging.info("processamento concluído com sucesso.")
        return result

    except Exception as lerr:
        l_error_log.error(f"erro no processamento: {lerr}")
        return None

if __name__ == "__main__":

    # gera nome de log de erro
    ls_error_log = lg.logger_generate_filename("carga_opmet", ".err")
    # logger
    # l_app_log, 
    l_error_log = lg.logger_setup("carga_opmet.log", ls_error_log)
    
    # inclui um caso que gera erro
    for item in [5, 0, 2]:
        process_data(item)

    # remove empty log
    lg.logger_check(ls_error_log)
    