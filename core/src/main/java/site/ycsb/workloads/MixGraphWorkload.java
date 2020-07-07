/**
 * Copyright (c) 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.workloads;

import site.ycsb.*;
import site.ycsb.generator.mixgraph.MixGraphGenerator;
import site.ycsb.generator.mixgraph.MixGraphKey;
import site.ycsb.generator.mixgraph.QueryDecider;
import site.ycsb.generator.mixgraph.Random64;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Vector;

/**
 * this workload is inspired by the paper "Characterizing, Modeling,
 * and Benchmarking RocksDB Key-Value Workloads at Facebook",
 * https://www.usenix.org/conference/fast20/presentation/cao-zhichao
 * the logic code is inspired by RocksDB.
 */

public class MixGraphWorkload extends CoreWorkload {
  // configuration attributes
  // example:  ./db_bench --benchmarks="mixgraph"
  // -use_direct_io_for_flush_and_compaction=true -use_direct_reads=true -cache_size=268435456
  // -keyrange_dist_a=14.18 -keyrange_dist_b=-2.917
  // -keyrange_dist_c=0.0164 -keyrange_dist_d=-0.08082 -keyrange_num=30
  // -value_k=0.2615 -value_sigma=25.45
  // -iter_k=2.517 -iter_sigma=14.236
  // -mix_get_ratio=0.85 -mix_put_ratio=0.14 -mix_seek_ratio=0.01
  // -sine_mix_rate_interval_milliseconds=5000 -sine_a=1000 -sine_b=0.000073 -sine_d=4500
  // --perf_level=2 # this is the io measurement report option, can be deleted
  // -reads=420000000 -num=50000000 -key_size=48
  // -mix_max_scan_len=10000
  // -keyrange_num=1

  public static final String KEYRANGE_DIST_A_PROPERTY = "keyrange_dist_a";
  public static final String KEYRANGE_DIST_A_DEFAULT = "0.0";
  public static final String KEYRANGE_DIST_B_PROPERTY = "keyrange_dist_b";
  public static final String KEYRANGE_DIST_B_DEFAULT = "0.0";
  public static final String KEYRANGE_DIST_C_PROPERTY = "keyrange_dist_c";
  public static final String KEYRANGE_DIST_C_DEFAULT = "0.0";
  public static final String KEYRANGE_DIST_D_PROPERTY = "keyrange_dist_d";
  public static final String KEYRANGE_DIST_D_DEFAULT = "0.0";

  public static final String KEYRANGE_NUM_PROPERTY = "keyrange_num";
  public static final String KEYRANGE_NUM_DEFAULT = "1";

  public static final String KEY_DIST_A_PROPERTY = "key_dist_a";
  public static final String KEY_DIST_A_DEFAULT = "0.0";
  public static final String KEY_DIST_B_PROPERTY = "key_dist_b";
  public static final String KEY_DIST_B_DEFAULT = "0.0";


  public static final String NUM_PROPERTY = "num";
  public static final String NUM_DEFAULT = "1000000";

  public static final String MIX_MAX_SCAN_LEN_PROPERTY = "mix_max_scan_len";
  public static final String MIX_MAX_SCAN_LEN_DEFAULT = "10000";

  public static final String MIX_GET_RATIO_PROPERTY = "mix_get_ratio";
  public static final String MIX_GET_RARIO_DEFAULT = "1.0";
  public static final String MIX_PUT_RATIO_PROPERTY = "mix_put_ratio";
  public static final String MIX_PUT_RATIO_DEFAULT = "0.0";
  public static final String MIX_SEEK_RATIO_PROPERTY = "mix_seek_ratio";
  public static final String MIX_SEEK_RATIO_DEFAULT = "0.0";

  public static final String VALUE_SIZE_MAX_PROPERTY = "value_size_max";
  public static final String VALUE_SIZE_MAX_DEFAULT = "102400";
  public static final String VALUE_SIZE_MIN_PROPERTY = "value_size_min";
  public static final String VALUE_SIZE_MIN_DEFAULT = "1";

  public static final String MIX_MAX_VALUE_SIZE_PROPERTY = "mix_max_value_size";
  public static final String MIX_MAX_VALUE_SIZE_DEFAULT = "1024";

  public static final String VALUE_SIZE_DISTRIBUTION_TYPE_E_PROPERTY = "value_size_distribution_type_e_property";
  public static final String VALUE_SIZE_DISTRIBUTION_TYPE_E_DEFAULT = "kFixed";

  public static final String VALUE_THETA_PROPERTY = "value_theta";
  public static final String VALUE_K_PROPERTY = "value_k";
  public static final String VALUE_SIGMA_PROPERTY = "value_sigma";

  public static final String VALUE_THETA_DEFAULT = "0.0";
  public static final String VALUE_K_DEFAULT = "0.0";
  public static final String VALUE_SIGMA_DEFAULT = "0.0";

  public static final String ITER_THETA_PROPERTY = "iter_theta";
  public static final String ITER_K_PROPERTY = "iter_k";
  public static final String ITER_SIGMA_PROPERTY = "iter_sigma";

  public static final String ITER_THETA_DEFAULT = "0.0";
  public static final String ITER_K_DEFAULT = "0.0";
  public static final String ITER_SIGMA_DEFAULT = "0.0";

  public static final String KEY_SIZE_PROPERTY = "key_size";
  public static final String KEY_SIZE_DEFAULT = "10";
  public static final String KEYS_PER_PREFIX_PROPERTY = "keys_per_prefix";
  public static final String PREFIX_SIZE_PROPERTY = "prefix_size";
  public static final String KEYS_PER_PREFIX_DEFAULT = "0";
  public static final String PREFIX_SIZE_DEFAULT = "0";


//  public static final String VALUE_SIZE_PROPERTY = "value_size";
//  public static final String VALUE_SIZE_DEFAULT = "100";


  final int default_value_max = 1024 * 1024;

  // parameters used in workload generation
  // operation counter, use the super class
  long read = 0;
  long gets = 0;
  long puts = 0;
  long found = 0;


  long seek = 0;
  long seek_found = 0;
  long bytes = 0;
  int value_max = default_value_max;
  double write_rate = 1000000.0;
  double read_rate = 1000000.0;
  boolean use_prefix_modeling = false;
  boolean use_random_modeling = false;
  Vector<Double> ratio = new Vector<>();
  long scan_len_max;
  double keyrange_dist_a;
  double keyrange_dist_b;
  double keyrange_dist_c;
  double keyrange_dist_d;
  long num;
  long keyrange_num;
  double key_dist_a;
  double key_dist_b;
  // The parameters of Generized Pareto Distribution "f(x)=(1/sigma)*(1+k*(x-theta)/sigma)^-(1/k+1)"
  double value_theta, value_k, value_sigma;

  QueryDecider query;

  MixGraphGenerator keysequence;
  double read_random_exp_range_;
  private double iter_theta, iter_k, iter_sigma;
  private int key_size, keys_per_prefix, prefix_size;


  long GetRandomKey(Random64 rand) {
    long rand_int = rand.Next();
    long key_rand;
    // only consider the situation without read_random_exp_range_ option.
    key_rand = rand_int % num;
    return key_rand;
  }

  @Override
  public void init(final Properties p) throws WorkloadException {
    System.out.println("Initial the parameters");

    super.init(p);
    ratio.add(Double.valueOf(p.getProperty(MIX_GET_RATIO_PROPERTY, MIX_GET_RARIO_DEFAULT)));
    ratio.add(Double.valueOf(p.getProperty(MIX_PUT_RATIO_PROPERTY, MIX_PUT_RATIO_DEFAULT)));
    ratio.add(Double.valueOf(p.getProperty(MIX_SEEK_RATIO_PROPERTY, MIX_SEEK_RATIO_DEFAULT)));

    scan_len_max = Long.valueOf(p.getProperty(MIX_MAX_SCAN_LEN_PROPERTY, MIX_MAX_SCAN_LEN_DEFAULT));
    keyrange_dist_a = Double.valueOf(p.getProperty(KEYRANGE_DIST_A_PROPERTY, KEYRANGE_DIST_A_DEFAULT));
    keyrange_dist_b = Double.valueOf(p.getProperty(KEYRANGE_DIST_B_PROPERTY, KEYRANGE_DIST_B_DEFAULT));
    keyrange_dist_c = Double.valueOf(p.getProperty(KEYRANGE_DIST_C_PROPERTY, KEYRANGE_DIST_C_DEFAULT));
    keyrange_dist_d = Double.valueOf(p.getProperty(KEYRANGE_DIST_D_PROPERTY, KEYRANGE_DIST_D_DEFAULT));
    num = Long.valueOf(p.getProperty(NUM_PROPERTY, NUM_DEFAULT));

    num = Math.max(super.recordcount, num);

    key_size = Integer.valueOf(p.getProperty(KEY_SIZE_PROPERTY, KEY_SIZE_DEFAULT));
    keys_per_prefix = Integer.valueOf(p.getProperty(KEYS_PER_PREFIX_PROPERTY, KEYS_PER_PREFIX_DEFAULT));
    prefix_size = Integer.valueOf(p.getProperty(PREFIX_SIZE_PROPERTY, PREFIX_SIZE_DEFAULT));

    keyrange_num = Integer.valueOf(p.getProperty(KEYRANGE_NUM_PROPERTY, KEYRANGE_NUM_DEFAULT));

    key_dist_a = Double.valueOf(p.getProperty(KEY_DIST_A_PROPERTY, KEY_DIST_A_DEFAULT));
    key_dist_b = Double.valueOf(p.getProperty(KEY_DIST_B_PROPERTY, KEY_DIST_B_DEFAULT));

    value_k = Double.valueOf(p.getProperty(VALUE_K_PROPERTY, VALUE_K_DEFAULT));
    value_sigma = Double.valueOf(p.getProperty(VALUE_SIGMA_PROPERTY, VALUE_SIGMA_DEFAULT));
    value_theta = Double.valueOf(p.getProperty(VALUE_THETA_PROPERTY, VALUE_THETA_DEFAULT));

    iter_k = Double.valueOf(p.getProperty(ITER_K_PROPERTY, ITER_K_DEFAULT));
    iter_sigma = Double.valueOf(p.getProperty(ITER_SIGMA_PROPERTY, ITER_SIGMA_DEFAULT));
    iter_theta = Double.valueOf(p.getProperty(ITER_THETA_PROPERTY, ITER_THETA_DEFAULT));

    Status s;
    if (value_max > Integer.valueOf(p.getProperty(MIX_MAX_VALUE_SIZE_PROPERTY, MIX_MAX_VALUE_SIZE_DEFAULT))) {
      value_max = Integer.valueOf(p.getProperty(MIX_MAX_VALUE_SIZE_PROPERTY, MIX_MAX_VALUE_SIZE_DEFAULT));
    }

    //TODO: check if we need a ReadOption here, refer to the TimeSeriesWorkload.
    char[] value_buffer;
    query = new QueryDecider();

    query.Initiate(ratio);
    keysequence = new MixGraphGenerator(num, key_dist_a, key_dist_b,
        keyrange_dist_a, keyrange_dist_b, keyrange_dist_c, keyrange_dist_d, keyrange_num,
        key_size, keys_per_prefix, prefix_size, getFieldLengthGenerator(p));
    // initiate finished

    // the limit of qps initiation
    // TODO: discuss whether we need RateLimiter or not?

    read_rate = 1000000.0; // how quick the read operation query stream is.
    write_rate = 100000.0;

  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    // TODO: replace this keysequence generation function to mixgraph workload
    // this will only be called in the load process
    String dbkey = keysequence.nextValue().getKeyString();
//    String dbkey = buildKeyName(keynum); we use rocksdb' generator
    HashMap<String, ByteIterator> values = super.buildValues(dbkey);

    Status status;
    int numOfRetries = 0;
    do {
      status = db.insert(table, dbkey, values);
      if (null != status && status.isOk()) {
        break;
      }
      // Retry if configured. Without retrying, the load process will fail
      // even if one single insertion fails. User can optionally configure
      // an insertion retry limit (default is 0) to enable retry.
      if (++numOfRetries <= insertionRetryLimit) {
        System.err.println("Retrying insertion, retry count: " + numOfRetries);
        try {
          // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
          int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          break;
        }
      } else {
        System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
            "Insertion Retry Limit: " + insertionRetryLimit);
        break;
      }
    } while (true);
    return null != status && status.isOk();
  }


  public void doTransactionRead(DB db, MixGraphKey key) {
    String keyname = key.getKeyString();

    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    } else if (dataintegrity) {
      // pass the full field list if dataintegrity is on for verification
      fields = new HashSet<String>(fieldnames);
    }

    HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
    db.read(table, keyname, fields, cells);

    if (super.dataintegrity) {
      verifyRow(keyname, cells);
    }

  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    MixGraphKey key = keysequence.nextValue();
    switch (query.GetType(key.rand_v)) {
      case 0:
        // get
        doTransactionRead(db, key);
        break;
      case 1:
        // put
        doTransactionInsert(db, key);
        break;
      case 2:
        // seek/scan
        doTransactionScan(db, key);
        break;
      default:
        // there is no other options
    }
    return true;
  }

  private void doTransactionScan(DB db, MixGraphKey key) {
    int scan_length = (int) (ParetoCdfInversion((double) key.rand_v / num, iter_theta, iter_k, iter_sigma)
        % scan_len_max);
    HashSet<String> fields = null;

    if (!readallfields) {
      // read a random field
      String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());

      fields = new HashSet<String>();
      fields.add(fieldname);
    }
    db.scan(table, key.getKeyString(), scan_length, fields, new Vector<HashMap<String, ByteIterator>>());

  }

  private void doTransactionInsert(DB db, MixGraphKey key) {
    int val_size = ParetoCdfInversion((double) key.rand_v / num, value_theta, value_k, value_sigma);
    if (val_size < 0) {
      val_size = 10;
    } else if (val_size > value_max) {
      val_size = val_size % value_max;
    }

    HashMap<String, ByteIterator> values = buildValues(key.getKeyString(), (int) (val_size / fieldcount));
    db.insert(table, key.getKeyString(), values);
  }

  protected HashMap<String, ByteIterator> buildValues(String key, int size) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      if (dataintegrity) {
        data = new StringByteIterator(buildFixedLengthDeterministicValue(key, fieldkey, size));
      } else {
        // fill with random data
        data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
      }
      values.put(fieldkey, data);
    }
    return values;
  }

  private String buildFixedLengthDeterministicValue(String key, String fieldkey, int size) {
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

  private int ParetoCdfInversion(double u, double theta, double k, double sigma) {
    double ret;
    if (k == 0.0) {
      ret = theta - sigma * Math.log(u);
    } else {
      ret = theta + sigma * (Math.pow(u, -1 * k) - 1) / k;
    }
    return (int) Math.ceil(ret);
  }
}
