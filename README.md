# Compile the RocksDB module only

use the following command:

`mvn -pl com.yahoo.ycsb:rocksdb-binding -am clean packages -DskipTests=true`

# The main steps you need to following

I built the system by following this [link](http://www.programmersought.com/article/2061668498/)

## Step 1. Compile the RocksDB on your own computer

the command is `make rocksdbjavastaticrelease -j8`, there might be some problem of showing `no such file`-like message, just go to the rocksdb's make file and delete whatever it said. Then, get the file in `java/target/rocksdbjni-version.number.jar`


## Step 2. Pick or write your own workload profile

refer to the workload profile provided by Yahoo! in `workload` directory, you can change the command options in following format

`./bin/ycsb load rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data`


## Step 3. Load and Run

There are two different action in YCSB, if you want to test the write performance or operations need preceeding data like updates, use `load` operation to generate the basic dataset, and test the read performance in `run` operation



