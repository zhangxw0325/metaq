#!/bin/sh
TM=`date +%s`
nohup sh $(dirname $0)/tool.sh com.taobao.metamorphosis.examine.DefaultProducer $@ 2>&1 >p_${TM}.log &



