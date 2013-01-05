export JAVA_HOME=/opt/taobao/java
export PATH=/opt/taobao/mvn/bin:$JAVA_HOME/bin:$PATH
mvn -Dmaven.test.skip=true clean package install assembly:assembly -U
