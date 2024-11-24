export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

export SPARK_HOME=/home/stephanelam/DataInteg/spark/spark-3.5.2-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH

export HADOOP_HOME=/home/stephanelam/DataInteg/Hadoop
export PATH=$HADOOP_HOME/bin:$PATH

./bin/hdfs namenode -format
./sbin/start-dfs.sh

Vérifiez que HDFS fonctionne :

Accédez à http://localhost:9870

(Suite du déroulement du projet sur le Compte Rendu)