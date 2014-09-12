#!/bin/bash
NAME=LogServer

#PWD=`pwd`
PWD="/home/lscm/codebase/sf1r-logserver/bin"
cd $PWD
CONSOLE_LOG_DIR="./consolelog"
mkdir -p $CONSOLE_LOG_DIR
TIMENOW=`date +%m-%d.%H%M`
LOG_FILE=$CONSOLE_LOG_DIR/$NAME.$TIMENOW.log
PROCESS_NUM=`ps -ef|grep "\./LogServerProcess"|grep -v grep|wc -l`
if [ $PROCESS_NUM -eq 1 ]
  then
    echo -e "$NAME has already started!\n"
    exit 0
  else
    echo -e "Starting $NAME..."
    ./LogServerProcess -F config/logserver.cfg > $LOG_FILE 2>&1 &
    sleep 4
    PROCESS_NUM=`ps -ef|grep "\./LogServerProcess"|grep -v grep|wc -l`
    if [ $PROCESS_NUM -eq 1 ]
      then
        echo -e "Success. \nTo monitor the log file, type: 'tail -f $LOG_FILE'\n"
      else
        echo -e "Fail.\nCheck log $LOG_FILE for more detail.\n"
    fi
fi
exit 0
