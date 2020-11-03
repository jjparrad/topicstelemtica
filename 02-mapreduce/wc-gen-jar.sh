# estas variables depende de cada instalación y distribución
#
# para el caso de HortonWorks 3.0:

HADOOP_HOME=/usr/hdp/current/hadoop-client
HADOOP_CONF_DIR=/usr/hdp/current/hadoop-client/etc/hadoop/
#
# Tambien cambian los nombres especificos de las librerias, revise en cada caso y haga los ajustes
#
HADOOP_COMMON_JAR=$HADOOP_HOME/client/hadoop-mapreduce-client-core.jar:$HADOOP_HOME/client/hadoop-common.jar

# para CLOUDERA 5.14.0
#HADOOP_HOME=/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24
#HADOOP_COMMON_JAR=$HADOOP_HOME/lib/hadoop/client/hadoop-mapreduce-client-core.jar:$HADOOP_HOME/lib/hadoop/hadoop-common.jar


mkdir wordcount_classes

javac -classpath $HADOOP_COMMON_JAR -d wordcount_classes WordCount.java
jar -cvf wc.jar -C wordcount_classes/ .

rm -rf wordcount_classes
