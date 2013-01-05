#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname opts"
  exit 1
fi

BASE_DIR=$(dirname $0)/..

CLASSPATH=.:${BASE_DIR}/conf:${CLASSPATH}

JAVA_OPT_1="-server -Xms4g -Xmx4g -Xmn2g -XX:PermSize=128m -XX:MaxPermSize=320m"
JAVA_OPT_2="-XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:SoftRefLRUPolicyMSPerMB=0 -XX:+CMSClassUnloadingEnabled -XX:SurvivorRatio=8 -XX:+DisableExplicitGC"
JAVA_OPT_3="-verbose:gc -Xloggc:${BASE_DIR}/logs/metaq_gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
JAVA_OPT_4="-Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
JAVA_OPT_5="-XX:-OmitStackTraceInFastThrow"
JAVA_OPT_6="-Djava.ext.dirs=${BASE_DIR}/lib -Dlog4j.configuration=${BASE_DIR}/bin/log4j.properties"
#JAVA_OPT_7="-Xdebug -Xrunjdwp:transport=dt_socket,address=9555,server=y,suspend=n"
JAVA_OPT_8="-cp ${CLASSPATH}"

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=/opt/taobao/java
fi

JAVA="$JAVA_HOME/bin/java"

JAVA_OPTS="${JAVA_OPT_1} ${JAVA_OPT_2} ${JAVA_OPT_3} ${JAVA_OPT_4} ${JAVA_OPT_5} ${JAVA_OPT_6} ${JAVA_OPT_7} ${JAVA_OPT_8}"

numactl --interleave=all $JAVA $JAVA_OPTS $@
