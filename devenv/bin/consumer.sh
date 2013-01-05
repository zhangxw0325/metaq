#!/bin/sh
TM=`date +%s`
nohup sh $(dirname $0)/tool.sh com.taobao.metamorphosis.examine.DefaultConsumer $@ 2>&1 >c_${TM}.log &

