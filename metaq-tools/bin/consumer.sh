#! /bin/bash

function help()
{
        echo "./comsumer.sh -t topic -g group"
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
LOGFILE=../logs.txt
nohup sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.fresh.Consumer20 $topic $group 2>&1 >>$LOGFILE &

tail -100f $LOGFILE
