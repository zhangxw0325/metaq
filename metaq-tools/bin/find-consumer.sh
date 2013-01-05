#! /bin/bash

#
# 查看topic有多少消费者
#

function help()
{
	echo "./find-consumer.sh -t topic"
}

while getopts "t:" options;do
	case $options in
		t)
			topic=$OPTARG;;
		/?)
			true			
	esac
done


if [ -z $topic ]; then
	help
	exit 1;
fi

#export ZK_SERVER="10.232.133.167:2181"
export ZK_SERVER="172.24.113.126:2181"

sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.fresh.FindConsumer $topic


