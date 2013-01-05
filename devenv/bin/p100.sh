#!/bin/sh

nohup sh $(dirname $0)/runclass.sh com.taobao.metamorphosis.examine.SimpleProducer $@ 2>&1 >p100.log &

