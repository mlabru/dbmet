#!/bin/bash

# language
# export LANGUAGE=pt_BR

# dbmet directory
DBMET=~/dbmet

# nome do computador
HOST=`hostname`

# get today's date
TDATE=`date '+%Y-%m-%d_%H-%M-%S'`

# home directory exists ?
if [ -d ${DBMET} ]; then
    # set home dir
    cd ${DBMET}
fi

# ckeck if another instance of loader is running
DI_PID_LOADER=`ps ax | grep -w python3 | grep -w carga_inmet.py | awk '{ print $1 }'`

if [ ! -z "$DI_PID_LOADER" ]; then
    # log warning
    echo "[`date`]: process loader is already running. Restarting..."
    # kill process
    kill -9 $DI_PID_LOADER
    # wait 3s
    sleep 3
fi

# set PYTHONPATH
export PYTHONPATH="$PWD/."

# executa o loader
python3 inmet/carga_inmet.py $@ > logs/carga_inmet.$HOST.$TDATE.log 2>&1 &

# < the end >----------------------------------------------------------------------------------
