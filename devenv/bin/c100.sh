#!/bin/sh

nohup sh $(dirname $0)/runclass.sh com.taobao.metamorphosis.examine.SimpleConsumer $@ 2>&1 >c100.log &

