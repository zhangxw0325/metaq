#! /bin/bash

function help()
{
        echo "./producer.sh -t topic"
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
LOGFILE=../logs.txt
nohup sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.fresh.Producer20 $topic 2>&1 >>$LOGFILE &

tail -100f $LOGFILE
