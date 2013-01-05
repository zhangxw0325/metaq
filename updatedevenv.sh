#!/bin/sh
svn up

sh install.sh

PWD=`pwd`

#sh ${PWD}/devenv/bin/end.sh

rm -rf ${PWD}/devenv/lib

rm -rf ${PWD}/devenv/logs

cp -R ${PWD}/target/metaq-server.dir/taobao-metaq/metaq-server/lib ${PWD}/devenv/

