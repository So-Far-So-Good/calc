run as:
hadoop jar calc.jar customer location from to wireid

Settings.HBaseHost must point to zookeeper
(or else org.apache.zookeeper.ClientCnxn - Session 0x0 for server null will be thrown)

/etc/hadoop/conf/hadoop-env.sh - uncomment export HADOOP_CLASSPATH and add /usr/lib/hbase/hbase.jar


if any problem with the host check:
/var/log/cloudera-scm-agent/cloudera-scm-agent.log