export JAVA_HOME=/opt/taobao/java
export PATH=/opt/taobao/mvn/bin:$JAVA_HOME/bin:$PATH
mvn -Dtest -DfailIfNoTests=false clean package assembly:assembly