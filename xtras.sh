#!/bin/bash

# language
# export LANGUAGE=pt_BR

# dbmet directory
DBMET=~/dbmet

# nome do computador
HOST=`hostname`

# get today's date
TDATE=`date '+%Y-%m-%d_%H-%M-%S'`

# log file
LOGF="logs/xtras.$HOST.$TDATE.log"

# home directory exists ?
if [ -d ${DBMET} ]; then
    # set home dir
    cd ${DBMET}
fi

# logger
echo "Início de processamento: " $(date '+%Y-%m-%d %H:%M') > $LOGF

# load config file
llst_dates=(`date --file=xtras.dat '+%Y-%m-%d'`)

# last processed date
#lst_date=$(<xtras.dat)

# last processed date
lst_date=${llst_dates[0]}

# stop date
stp_date=${llst_dates[1]}

# initial date
# ini_date=$(date -d "${lst_date} +1 day" +%Y-%m-%d)
ini_date=${llst_dates[0]}

# final date: add 2 days to last date
# fin_date=$(date --date="${lst_date} +2 day" '+%Y-%m-%d')
fin_date=$(date -d "${lst_date} +2 day" +%Y-%m-%d)

# format to compare
dt_fin=`date --date=${fin_date} '+%Y%m%d'`
dt_stp=$(date -d "${stp_date}" '+%Y%m%d')

# in range to process ?
if [[ $dt_fin -lt $dt_stp ]]; then

    # logger
    echo "Processando de ${ini_date} até ${fin_date}" >> $LOGF

    # process next 2 days
    bash ./opmet.sh -i ${ini_date}T00:00 -f ${fin_date}T00:00 -x -t iepv

    # save final date
    echo $fin_date >  xtras.dat
    # save stop date
    echo $stp_date >> xtras.dat

    # logger
    echo "Fim de processamento: " $(date '+%Y-%m-%d %H:%M') >> $LOGF
fi

# < the end >----------------------------------------------------------------------------------
