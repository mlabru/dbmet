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

# log file
LOGF="logs/mfas.$HOST.$TDATE.log"

# logger
echo "Início de processamento: " $(date '+%Y-%m-%d %H:%M') > $LOGF

# ckeck if another instance of loader is running
DI_PID_LOADER=`ps ax | grep -w python3 | grep -w mfas_load.py | awk '{ print $1 }'`

if [ ! -z "$DI_PID_LOADER" ]; then
    # log warning
    echo "[`date`]: process loader is already running. Waiting..." >> $LOGF
    # kill process
    kill -9 $DI_PID_LOADER
    # wait 10s
    sleep 10
fi

# set PYTHONPATH
export PYTHONPATH="$PWD/."
# executa o loader
python3 mfas/mfas_load.py $@ >> $LOGF 2>&1

# logger
echo "Fim de processamento: " $(date '+%Y-%m-%d %H:%M') >> $LOGF

# < the end >----------------------------------------------------------------------------------
