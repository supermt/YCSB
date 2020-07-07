#!/usr/bin/bash
# compile first
mvn -pl site.ycsb:rocksdb-binding -am package -DskipTests=true -Dcheckstyle.skip

./bin/ycsb.sh load rocksdb -s -P workloads/workload_mixgraph -p rocksdb.dir=/tmp/ycsb-rocksdb-data

./bin/ycsb.sh run rocksdb -s -P workloads/workload_mixgraph -p rocksdb.dir=/tmp/ycsb-rocksdb-data
