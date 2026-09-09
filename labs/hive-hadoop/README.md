# 🗄️ Hive and Hadoop HDFS compatibility lab

This runs actual HDFS NameNode/DataNode processes and HiveServer2 inside one disposable `apache/hive:4.0.0` container. The image includes Hadoop. It is not a mock filesystem, but it is also not a multi-host cluster.

The shared batch exports two valid current events. They are uploaded into an HDFS partition directory. Hive registers an external text table with a `batch_date` partition and reads its rows. The verifier repeats DDL, compares rows and total quantity, drops only the lab table, checks that its external data remains, and recreates the table.

## Run

Prefer the `hive-hadoop` job of [platform CI](../../.github/workflows/platform-ci.yml). For Linux with Docker, from the repository root:

```sh
python labs/hive-hadoop/prepare.py /tmp/portfolio-hive-input
docker run -d --name portfolio-hive --hostname portfolio-hive --memory=4g \
  -v "$PWD:/lab:ro" -v /tmp/portfolio-hive-input:/input:ro \
  -e HADOOP_CONF_DIR=/lab/labs/hive-hadoop -e HIVE_CONF_DIR=/lab/labs/hive-hadoop \
  --entrypoint bash apache/hive:4.0.0 /lab/labs/hive-hadoop/bootstrap.sh
python labs/hive-hadoop/verify.py
docker logs portfolio-hive
```

Cleanup only the named disposable container when finished:

```sh
docker rm -f portfolio-hive
```

This removes the lab's HDFS and metastore data. Do not mount existing HDFS storage. Bootstrap checks that it is inside a container and that its dedicated NameNode path is not already formatted. It opens no host ports.

## Boundaries

Replication=1, embedded Derby metastore, no Kerberos, no TLS, no YARN cluster, no HA or performance claims. Text format is used for inspectable fixtures, not claimed as an optimal production format. Dropping an external table preserves data here; production behavior also depends on table properties and storage policies.

Interview: Which component knows block locations? Which knows table columns? What is the difference between a partition directory and metastore partition registration? What breaks if this single DataNode is lost?

Sources: [Apache Hive Docker setup](https://hive.apache.org/docs/latest/admin/setting-up-hive-with-docker/), [Hadoop single-node setup](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html).
