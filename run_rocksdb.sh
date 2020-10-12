#!/usr/bin/bash
# compile first
mvn -pl site.ycsb:rocksdb-binding -am package -DskipTests=true -Dcheckstyle.skip

./bin/ycsb.sh load rocksdb -s -P workloads/rocksdb_dota_workload -p rocksdb.dir=/home/jinghuan/rocksdb_nvme

# ./bin/ycsb.sh run rocksdb -s -P workloads/rocksdb_dota_workload -p rocksdb.dir=/home/jinghuan/rocksdb_nvme
