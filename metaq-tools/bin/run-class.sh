#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

CLASSPATH=$CLASSPATH:$base_dir/conf


if [ -z "$META_TOOLS_OPTS" ]; then
  META_TOOLS_OPTS="-Djava.ext.dirs=$base_dir/lib -Dlog4j.configuration=$base_dir/bin/log4j.properties"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=/opt/taobao/java
fi

JAVA="$JAVA_HOME/bin/java"

#META_TOOLS_OPTS="$META_TOOLS_OPTS -verbose"
#META_TOOLS_OPTS="$META_TOOLS_OPTS -Djava.awt.headless=true  -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8786"
#echo "--------------------------"
#echo $META_TOOLS_OPTS
#echo "--------------------------"

$JAVA $META_TOOLS_OPTS -classpath $CLASSPATH $@
