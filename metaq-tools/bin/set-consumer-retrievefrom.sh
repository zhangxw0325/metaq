#! /bin/bash

#
# 设置消费端从主机还是备机上取消息
#

function help()
{
	echo "Usage:"
	echo "recieve from slave, slave is greater than -1"
	echo "	./set-consumer-retrievefrom.sh -g group -v slaveId"
	echo "recieve from master"
	echo "	./set-consumer-retrievefrom.sh -g group -v -1"
	echo "invalid server configuration"
	echo "	./set-consumer-retrievefrom.sh -g group -v null"
}

while getopts "v:g:" options;do
	case $options in
		v)
			slaveid=$OPTARG;;
		g)
			group=$OPTARG;;
		/?)
			true			
	esac
done


if [ -z $slaveid ]; then
	help
	exit 1;
elif [ -z $group ]; then
	help
        exit 1;
fi

#export ZK_SERVER="10.232.133.167:2181"
export ZK_SERVER="172.24.113.126:2181"

sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.fresh.ConsumerRetrieveFrom $group $slaveid


