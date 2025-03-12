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
DI_PID=`ps ax | grep -w python3 | grep -w radar_image.py | awk '{ print $1 }'`

if [ ! -z "$DI_PID" ]; then
    # log warning
    echo "[`date`]: process downloader is already running. Restarting..."
    # kill process
    kill -9 $DI_PID
    # wait 3s
    sleep 3
fi

# set PYTHONPATH
export PYTHONPATH="$PWD/."

# executa o downloader
python3 radar/radar_image.py $@ > logs/radar_image.$HOST.$TDATE.log 2>&1 &

# < the end >----------------------------------------------------------------------------------
