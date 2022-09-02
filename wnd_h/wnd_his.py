# -*- coding: utf-8 -*-
"""
wind history

2022.sep  mlabru   initial version (Linux/Python)
"""
# < imports >----------------------------------------------------------------------------------

# python library
import glob
import logging
import multiprocessing
import os
import shutil
import sys

# patoolib
import patoolib

# local
import wnd_h.wnd_db as db
import wnd_h.wnd_defs as df

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def trata_dir(fs_dir: str):
    """
    trata diretório

    :param fs_dir (str): diretório
    """
    # logger
    M_LOG.info(">> trata_dir")

    # check input
    assert fs_dir

    # split dir
    llst_dir = fs_dir.split('/')

    # localidade
    ls_loc = llst_dir[-3]
    assert ls_loc in df.DLST_SITES

    # walk directories...
    for lroot, ldirs, lfiles in os.walk(fs_dir, followlinks=False):
        # for all files...
        for lfile in lfiles:
            # WIND* history file ?
            if lfile.lower().startswith("wind") and lfile.lower().endswith(".his"):
                # MPS file ?
                if lfile.lower().startswith("wind_mps"):
                    # despreza
                    continue

                # trata file
                trata_file(ls_loc, os.path.join(lroot, lfile))

    # remove o diretório
    shutil.rmtree(fs_dir)

# ---------------------------------------------------------------------------------------------
def trata_file(fs_loc: str, fs_file: str):
    """
    trata file

    :param fs_loc (str): localidade
    :param fs_file (str): archive
    """
    # logger
    M_LOG.info(">> trata_file")

    # check input
    assert fs_loc in df.DLST_SITES
    assert fs_file

    # inicia contador de linhas
    li_lcount = 0

    # create MongoDB connection
    l_mongo_client = db.mongo_connect()
    assert l_mongo_client

    # open file
    with open(fs_file, 'r') as lfh:
        # header len
        li_head_len = 0
        
        try:
            # scan lines...
            for lline in lfh:
                # first line ?
                if 0 == li_lcount:
                    # history file ?
                    if lline.lower().startswith("history file"):
                        # inc line counter
                        li_lcount += 1
                        # skip line
                        continue

                    # senão,...
                    else:
                        # logger
                        M_LOG.error("invalid history file for %s. Skipping file.", fs_file)
                        # return error
                        return

                # second line ?
                if 1 == li_lcount:
                    # header ?
                    if lline.lower().startswith("createdate"):
                        # split header
                        llst_head, li_head_len, lc_sep = trata_header(lline.strip())

                        # inc line counter
                        li_lcount += 1
                        # next line
                        continue

                    # senão,...
                    else:
                        # logger
                        M_LOG.error("invalid header for %s. Skipping file.", fs_file)
                        # return error
                        return

                # split line
                llst_line = lline.strip().split(lc_sep)
                # line len
                li_line_len = len(llst_line)

                # line is valid ?
                if li_line_len != li_head_len:
                    # logger
                    M_LOG.error("invalid line %d. Expected %d, found %d. Skipping line.",
                                  li_lcount, li_head_len, li_line_len)

                    # inc line counter
                    li_lcount += 1
                    # skip line
                    continue

                # send line to mongo
                trata_line(l_mongo_client, fs_loc, llst_head, llst_line)

                # inc line counter
                li_lcount += 1

        # em caso de erro...
        except Exception as lerr:
            # logger
            M_LOG.error("%s", str(lerr))
            
    # logger
    M_LOG.warning("%d lines processed.", li_lcount)

    # close MongoDB connection
    l_mongo_client.close()

# ---------------------------------------------------------------------------------------------
def trata_header(fs_line: str):
    """
    trata header

    :param fs_line (str): registro de dados
    """
    # logger
    M_LOG.info(">> trata_header")

    # check input
    assert fs_line

    # split header
    llst_head = fs_line.split('\t')

    # wrong separator ?
    if 1 == len(llst_head):
        # split header
        llst_head = fs_line.split(',')
        # return
        return llst_head, len(llst_head), ','

    # return
    return llst_head, len(llst_head), '\t'

# ---------------------------------------------------------------------------------------------
def trata_line(f_mongo_client, fs_loc: str, flst_head: list, flst_line: list):
    """
    trata line

    :param f_mongo_client: MongoDB client
    :param fs_loc (str): localidade
    :param flst_head (str): file header
    :param fs_line (str): data register
    """
    # logger
    M_LOG.info(">> trata_line")

    # check input
    assert f_mongo_client
    assert fs_loc in df.DLST_SITES
    assert len(flst_head) == len(flst_line)

    # create dict
    ldct_reg = dict(zip(flst_head, flst_line))
    M_LOG.debug("loc: %s  registro: %s", fs_loc, str(ldct_reg))

    # save collection on Mongo DB
    db.save_data(f_mongo_client, fs_loc, ldct_reg)

# ---------------------------------------------------------------------------------------------
def main():
    """
    drive app
    """
    # logger
    M_LOG.info(">> main")

    # initial processes list
    llst_prc = []

    # walk directories...
    for lroot, ldirs, lfiles in os.walk(df.DS_DATA_DIR, followlinks=True):
        # for all files...
        for lfile in lfiles:
            # not a compressed file ?
            if not (lfile.lower().endswith("7z") or
                    lfile.lower().endswith("rar") or \
                    lfile.lower().endswith("zip")):
                # next file
                continue

            # diretório de extração
            ls_dir_out = os.path.join(lroot, lfile.lower().replace(' ', '_').split('.')[0])

            # diretório não existe ?
            if not os.path.exists(ls_dir_out):
                # cria diretório
                os.makedirs(ls_dir_out)

            try:
                # extract archive
                patoolib.extract_archive(os.path.join(lroot, lfile), outdir=ls_dir_out)

            # em caso de erro...
            except Exception as lerr:
                # ignora erros
                pass

            # cria um processo para tratar o diretório
            lprc = multiprocessing.Process(target=trata_dir, args=(ls_dir_out,))
            # start do processo
            lprc.start()

            # salva o processo
            llst_prc.append(lprc)

    # para todos os processos...
    for lprc in llst_prc:
        # aguarda o término do processo
        lprc.join()

# ---------------------------------------------------------------------------------------------
# this is the bootstrap process

if "__main__" == __name__:
    # logger
    logging.basicConfig(datefmt="%Y/%m/%d %H:%M",
                        format="%(asctime)s %(message)s",
                        level=df.DI_LOG_LEVEL)

    # disable logging
    # logging.disable(sys.maxsize)

    try:
        # run application
        main()

    # em caso de erro...
    except KeyboardInterrupt:
        # logger
        logging.warning("Interrupted.")

    # terminate
    sys.exit(0)

# < the end >----------------------------------------------------------------------------------
