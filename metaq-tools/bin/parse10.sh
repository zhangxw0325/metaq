#! /bin/bash

function help()
{
        echo "./producer.sh -f file"
}

while getopts "f:" options;do
        case $options in
                f)
                        file=$OPTARG;;
                /?)
                        true
        esac
done


if [ -z $file ]; then
        help
        exit 1;
fi
LOGFILE=../logs.txt
nohup sh $(dirname $0)/run-class.sh com.taobao.metamorphosis.tools.fresh.ParseMessage10 $file 2>&1 >>$LOGFILE &

tail -100f $LOGFILE
