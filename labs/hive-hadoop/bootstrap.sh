#!/usr/bin/env bash
set -euo pipefail
# Only run inside the disposable lab container; never format a user's cluster.
test -f /.dockerenv
test ! -e /tmp/portfolio-hdfs/name/current
export HADOOP_CONF_DIR=/lab/labs/hive-hadoop
export HIVE_CONF_DIR=/lab/labs/hive-hadoop
export HADOOP_LOG_DIR=/tmp/portfolio-hadoop-logs
mkdir -p "$HADOOP_LOG_DIR" /tmp/portfolio-hdfs
hdfs namenode -format -nonInteractive
hdfs --daemon start namenode
hdfs --daemon start datanode
for attempt in $(seq 1 60); do
  if hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)'; then
    break
  fi
  sleep 2
done
hdfs dfsadmin -report | grep 'Live datanodes (1)'
hdfs dfsadmin -safemode wait
hdfs dfs -mkdir -p /tmp /warehouse /portfolio/events/batch_date=2026-01-02
hdfs dfs -put /input/events.tsv /portfolio/events/batch_date=2026-01-02/events.tsv
schematool -dbType derby -initSchema
exec hiveserver2
