#! /bin/bash

#
# 查看各个订阅组消费进度
#

function help()
{
        echo "./run.sh -t topic -g group"
}

while getopts "t:g:" options;do
	case $options in
		t)
			topic=$OPTARG;;
		g)
			group=$OPTARG;;
		/?)
			true			
	esac
done


if [ -z $topic ]; then
	help
	exit 1;
elif [ -z $group ]; then
	help
        exit 1;
fi

#export ZK_SERVER="10.232.133.167:2181"
export ZK_SERVER="172.24.113.126:2181"

sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.fresh.ConsumerState $group $topic


