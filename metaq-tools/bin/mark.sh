#! /bin/bash

function help()
{
	echo "./run.sh -l topics -b bids"
}

while getopts "l:b:" options;do
	case $options in
		l)
			topics=$OPTARG;;
		b)
			bids=$OPTARG;;
		/?)
			true
	esac
done

if [ -z $topics ]; then
	help
	exit 1;
elif [ -z $bids ]; then
	bids=""
fi

export ZK_SERVER="172.24.113.126:2181"

sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.fresh.Mark $topics $bids

