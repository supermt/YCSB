# Compile the modules

use the following command:

if you don't want to skip the checkstyle:
`mvn -pl site.ycsb:rocksdb-binding -am package -DskipTests=true`

to skip the style checking
`mvn -pl site.ycsb:cassandra-binding -am package -DskipTests=true -Dcheckstyle.skip`

# The main steps you need to following

I built the system by following this [link](http://www.programmersought.com/article/2061668498/)

## Step 1. Compile the RocksDB on your own computer

install the java and cmake in your server first.

```bash
sudo apt-get install vagrant,default-jdk,cmake
```


the command is `make rocksdbjavastaticrelease -j8`, there might be some problem of showing `no such file`-like message, just go to the rocksdb's make file and delete whatever it said. Then, get the file in `java/target/rocksdbjni-version.number.jar`

In some cases you may meet "fatal error jni.h no such file or directory", add the following line in your ~/.bashrc (ubuntu) or other configuration files for your terminal

```bash
export JAVA_INCLUDE_DIR="/usr/lib/jvm/default-java/include;/usr/lib/jvm/default-java/include/linux"
```

**DO Remember to change the version number of the file name into the version number in `pom.xml`**


## Step 2. Pick or write your own workload profile

refer to the workload profile provided by Yahoo! in `workload` directory, you can change the command options in following format
~~`./bin/ycsb load rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data`~~

If you want to start the YCSB with some JAVA options to config JVM or Debugging, use `/bin/ycsb.sh load rocksdb -s -P workloads/workloada -p rocksdb.dir=/tmp/ycsb-rocksdb-data`. Just using shell is okay, but do remember install Python on your computer.

> visit this link [issue](https://github.com/brianfrankcooper/YCSB/pull/908) to complete the dependency.

## Step 3. Load and Run

There are two different action in YCSB, if you want to test the write performance or operations need preceeding data like updates, use `load` operation to generate the basic dataset, and test the read performance in `run` operation

# How to combine the RocksDB I modified into YCSB?

Just copy the jar file I mentioned in `Step 1.` above into the directory `YCSB/rocksdb/target/dependency` and remove the old ones. And remember! **Make sure the version is the same.**

# Features:

# About real-world data
We integrate the file workload from [Terark](https://github.com/Terark/YCSB.git), it provides a `file workload` to load real-world data

and here we use [Web data: Amazon movie reviews](https://snap.stanford.edu/data/movies.txt.gz) to generate the data. 

```bash
git clone https://github.com/Terark/amazon-movies-parser
cd amazon-movies-parser
g++ -o parser amazon-moive-parser.cpp -std=c++11
./parser /path/to/movies.txt /path/to/movies_flat.txt
```
