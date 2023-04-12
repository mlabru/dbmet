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
import typing

# patoolib
import patoolib

sys.path.insert(1, "..")

# local
import mfas_db as db
import mfas_defs as df

# < logging >----------------------------------------------------------------------------------

M_LOG = logging.getLogger(__name__)
M_LOG.setLevel(df.DI_LOG_LEVEL)

# ---------------------------------------------------------------------------------------------
def archives():
    """
    trata archives
    """
    # logger
    M_LOG.info(">> archives")

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
            lprc = multiprocessing.Process(target=dir, args=(ls_dir_out,))
            # start do processo
            lprc.start()

            # salva o processo
            llst_prc.append(lprc)

    # para todos os processos...
    for lprc in llst_prc:
        # aguarda o término do processo
        lprc.join()

# ---------------------------------------------------------------------------------------------
def data_block(f_mongo_client, f_fh: typing.TextIO,
               fs_code: str, fi_vd_len: int, fs_date: str, fs_time: str):
    """
    process lines on file

    :param f_mongo_client: MongoDB client
    :param f_fh (_io.TextIOWrapper): file handler
    :param fs_code (str): sigla da localidade
    :param fi_vd_len (int): variable definition len
    """
    # logger
    M_LOG.info(">> data_block")

    # check input
    # assert f_mongo_client
    assert f_fh
    assert fs_code
    assert fi_vd_len

    # create registers list
    llst_regs = []

    # read first detail header
    lline = f_fh.readline()

    # detail header ?
    if not lline.startswith("#"):
        # return error
        return

    # split first detail headers
    llst_headers = lline[1:].strip().split()

    # read first detail line
    lline = f_fh.readline()

    # have 2 headers ?
    if "PG" == llst_headers[0]:
        # split detail line
        llst_detail_pg = lline.strip().split()

        # clear to go
        assert len(llst_headers) == len(llst_detail_pg)

        # read second detail header
        lline = f_fh.readline()

        # detail header ?
        if not lline.startswith("#"):
            # return error
            return
        
        # split second detail headers
        llst_headers += lline[1:].strip().split()

        # read second detail line
        lline = f_fh.readline()

    # senão,...
    else:
        # detail line
        llst_detail_pg = []
        
    # process all details...
    while lline and len(lline) > 1:  # not lline.startswith(fs_date):
        # split second detail line
        llst_detail = llst_detail_pg + lline.strip().split()

        # clear to go
        assert len(llst_headers) == len(llst_detail)
        assert len(llst_detail) == fi_vd_len + 1

        # create dict
        ldct_reg = dict(zip(llst_headers, llst_detail))
        # append to registers list
        llst_regs.append(ldct_reg)
        
        # read next line
        lline = f_fh.readline()

    # build data dictionary
    ldct_data = {"datahora": f"{fs_date} {fs_time}", "vento": llst_regs}

    # save collection on Mongo DB
    # db.save_data(f_mongo_client, ldct_data, fs_code)
    # M_LOG.debug("loc: %s  registro: %s", fs_code, str(ldct_data))
        
# ---------------------------------------------------------------------------------------------
def load_headers(f_mongo_client, f_fh: typing.TextIO, fs_file: str) -> None:
    """
    load headers & data blocks

    :param f_mongo_client: MongoDB client
    :param f_fh (_io.TextIOWrapper): file handler
    :param fs_file (str): filename
    """
    # logger
    M_LOG.info(">> load_headers")

    # check input
    assert f_fh

    # file date
    ls_date: str = "@@@"
    
    # read first line
    lline = f_fh.readline()
    # scan line...
    while lline and not lline.startswith("FORMAT-1"):
        # read next line
        lline = f_fh.readline()

    # file format ?
    if lline.startswith("FORMAT-1"):
        # read next line (date line)
        lline = f_fh.readline()

        # date, time, incr
        llst_date = lline.split()

        # date & time
        ls_date = llst_date[0]
        ls_time = llst_date[1]
        
        # read next line (MFAS)
        lline = f_fh.readline()

        # MFAS file ?
        if not lline.startswith("MFAS"):
            # logger
            M_LOG.error("invalid header. MFAS not found for %s. Skipping file.", fs_file)
            # raise an error
            raise Exception("MFAS not found.")

        # read next line (indexes)
        lline = f_fh.readline()

        # file information (len), variable definitions (len), 50
        llst_indx = lline.split()

        # information & definitions
        li_fi_len = int(llst_indx[0])
        li_vd_len = int(llst_indx[1])

    # senão,...
    else:
        # logger
        M_LOG.error("invalid format for %s. Skipping file.", fs_file)
        # raise an error
        raise Exception("invalid format.")

    # read next line
    lline = f_fh.readline()
    # skip lines...
    while lline and not lline.startswith("station code"):
        # read next line
        lline = f_fh.readline()

    # station code ?
    if lline.startswith("station code"):
        # split line
        llst_code = lline.split()

        # station code
        ls_code = llst_code[3]
        assert ls_code in df.DLST_SITES

    # senão,...
    else:
        # logger
        M_LOG.error("invalid station code for %s. Skipping file.", fs_file)
        # raise an error
        raise Exception("invalid station code.")

    # read next line
    lline = f_fh.readline()

    # while not EOF...
    while lline:
        # skip lines...
        while lline and not lline.startswith(ls_date):
            # read next line
            lline = f_fh.readline()

        # EOF ?
        if not lline:
            break

        # data block ?
        if lline.startswith(ls_date):
            # date, time, incr
            llst_date = lline.split()

            # date & time
            assert ls_date == llst_date[0]
            ls_time = llst_date[1]

        # senão,...
        else:
            # logger
            M_LOG.error("invalid date data for %s. Skipping file.", fs_file)
            # raise an error
            raise Exception("invalid date data.")

        # process data blocks
        data_block(f_mongo_client, f_fh, ls_code, li_vd_len, ls_date, ls_time)

        # read next line
        lline = f_fh.readline()

# ---------------------------------------------------------------------------------------------
def parse_file(fs_file: str) -> None:
    """
    parse file

    :param fs_file (str): file to parse
    """
    # logger
    M_LOG.info(">> parse_file")

    # check input
    assert fs_file

    # create MongoDB connection
    l_mongo_client = None  # db.mongo_connect()
    # assert l_mongo_client

    try:
        # open file
        with open(fs_file, 'r') as lfh:
            # get date and skip headers
            load_headers(l_mongo_client, lfh, fs_file)

    # em caso de erro...
    except Exception as lerr:
        # logger
        M_LOG.error("%s", str(lerr))
            
    # close MongoDB connection
    # l_mongo_client.close()

# ---------------------------------------------------------------------------------------------
def walk_dir(fs_dir: str) -> None:
    """
    trata diretório

    :param fs_dir (str): diretório
    """
    # logger
    M_LOG.info(">> walk_dir")

    # check input
    assert fs_dir

    # split dir
    llst_dir = fs_dir.split('/')

    # walk directories...
    for lroot, ldirs, lfiles in os.walk(fs_dir, followlinks=False):
        # for all files...
        for lfile in lfiles:
            # MND file ?
            if lfile.lower().endswith(".mnd"):
                # trata file
                parse_file(os.path.join(lroot, lfile))

    # remove o diretório
    # shutil.rmtree(fs_dir)

# ---------------------------------------------------------------------------------------------
def main() -> None:
    """
    drive app
    """
    # logger
    M_LOG.info(">> main")

    # trata diretório
    walk_dir(df.DS_DATA_DIR)

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
