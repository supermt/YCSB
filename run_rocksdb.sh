#!/usr/bin/bash
# compile first
mvn -pl com.yahoo.ycsb:rocksdb-binding -am package -DskipTests=true -Dcheckstyle.skip

./bin/ycsb.sh load rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data
