#!/bin/bash

ulimit -c unlimited

mkdir $(dirname $0)/../logs

LOGFILE=$(dirname $0)/../logs/meta_server.log

nohup sh $(dirname $0)/runclass.sh com.taobao.metamorphosis.ServerStartup $@ 2>&1 >>$LOGFILE &

sleep 1

tail $LOGFILE -f
