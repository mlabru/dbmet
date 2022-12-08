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
def trata_detail(f_mongo_client, fs_loc: str, flst_head: list, flst_line: list):
    """
    trata line

    :param f_mongo_client: MongoDB client
    :param fs_loc (str): localidade
    :param flst_head (str): file header
    :param fs_line (str): data register
    """
    # logger
    M_LOG.info(">> trata_detail")

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

    # walk directories...
    for lroot, ldirs, lfiles in os.walk(fs_dir, followlinks=False):
        # for all files...
        for lfile in lfiles:
            # debug
            M_LOG.debug("lfile: %s", str(lfile))
            # MND file ?
            if lfile.lower().endswith(".mnd"):
                # trata file
                trata_file(os.path.join(lroot, lfile))

    # remove o diretório
    shutil.rmtree(fs_dir)

# ---------------------------------------------------------------------------------------------
def trata_file(fs_file: str):
    """
    trata file

    :param fs_file (str): archive
    """
    # logger
    M_LOG.info(">> trata_file")

    # check input
    assert fs_file

    # create MongoDB connection
    #l_mongo_client = db.mongo_connect()
    #assert l_mongo_client

    try:
        # open file
        with open(fs_file, 'r') as lfh:
            # get date and skip header
            pos = trata_header(fs_file, lfh)

            print(pos)
            print("------------------------------------------------------")

            # process data lines
            trata_lines(lfh, pos)

    # em caso de erro...
    except Exception as lerr:
        # logger
        M_LOG.error("%s", str(lerr))
            
    # close MongoDB connection
    #l_mongo_client.close()

# ---------------------------------------------------------------------------------------------
def trata_header(fs_file: str, f_fh: typing.TextIO):
    """
    in header

    :param fs_file (str): filename
    :param f_fh (_io.TextIOWrapper): file handler

    :returns (bool): True if in header section False otherwise.
    """
    # logger
    M_LOG.info(">> trata_header")

    # check input
    assert f_fh

    # file date
    ls_date: str = "@@@"
    
    # init line counter
    li_lno = 0
    # read first line
    lline = f_fh.readline()
    # scan line...
    while lline:
        print(li_lno, "  -  ", f_fh.tell())
        # first line (format)?
        if 0 == li_lno:
            # file format ?
            if not lline.startswith("FORMAT-1"):
                # logger
                M_LOG.error("invalid format for %s. Skipping file.", fs_file)
                # raise an error
                raise Exception("invalid format.")

        # second line (date, time & incr) ?
        elif 1 == li_lno:
            # data
            llst_date = lline.split()

            # data dos dados
            ls_date = f"{llst_date[0]} {llst_date[1]}"
            M_LOG.debug("ls_date: %s", str(ls_date))

            # read next line
            lline = f_fh.readline()
            # next line
            li_lno += 1

            # next
            continue

        # third line (MFAS) ?
        elif 2 == li_lno:
            # MFAS file ?
            if not lline.startswith("MFAS"):
                # logger
                M_LOG.error("invalid header. MFAS not found for %s. Skipping file.", fs_file)
                # raise an error
                raise Exception("MFAS not found.")

        # skip lines
        if lline.startswith(ls_date):
            print(lline)
            # return
            return f_fh.tell()

        # read first line
        lline = f_fh.readline()
        # next line
        li_lno += 1

    # logger
    M_LOG.error("invalid header. Date not found for %s. Skipping file.", fs_file)
    # raise an error
    raise Exception("date not found.")

# ---------------------------------------------------------------------------------------------
def trata_lines(f_fh: typing.TextIO, pos: int):
    """
    process lines on file

    :param f_fh (_io.TextIOWrapper): file handler

    :returns (bool): True if in header section False otherwise.
    """
    # logger
    M_LOG.info(">> trata_header")

    # check input
    assert f_fh

    print(pos)
    f_fh.seek(pos)
    print(f_fh.tell())
    
    # read first line
    lline = f_fh.readline()
    # scan line...
    while lline:
        '''
        # split line
        llst_line = lline.strip().split(lc_sep)
        # line len
        li_line_len = len(llst_line)

        # line is valid ?
        if li_line_len != li_head_len:
            # logger
            M_LOG.error("invalid line %d. Expected %d, found %d. Skipping line.",
                        li_lno, li_head_len, li_line_len)

            # skip line
            continue

        # send line to mongo
        trata_detail(l_mongo_client, fs_loc, llst_head, llst_line)
        '''
        print(lline)
        # read first line
        lline = f_fh.readline()
        
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
