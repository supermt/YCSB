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

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.Generator;
import com.yahoo.ycsb.generator.mixgraph.*;

import java.util.*;

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
  public static final String VALUE_SIZE_MIN_PROPERTY = "value_size_max";
  public static final String VALUE_SIZE_MIN_DEFAULT = "1";

  public static final String MIX_MAX_VALUE_SIZE_PROPERTY = "mix_max_value_size";
  public static final String MIX_MAX_VALUE_SIZE_DEFAULT = "1024";

  public static final String VALUE_SIZE_DISTRIBUTION_TYPE_E_PROPERTY = "value_size_distribution_type_e_property";
  public static final String VALUE_SIZE_DISTRIBUTION_TYPE_E_DEFAULT = "kFixed";

  public static final String VALUE_SIZE_PROPERTY = "value_size";
  public static final String VALUE_SIZE_DEFAULT = "100";


  final long default_value_max = 1024 * 1024;

  // parameters used in workload generation
  long read = 0;  // including single gets and Next of iterators
  long gets = 0;
  long puts = 0;
  long found = 0;
  long seek = 0;
  long seek_found = 0;
  long bytes = 0;
  long value_max = default_value_max;
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

  MixGraphGenerator keysequence;
  double read_random_exp_range_;


  long GetRandomKey(Random64 rand) {
    long rand_int = rand.Next();
    long key_rand;
    // only consider the situation without read_random_exp_range_ option.
    key_rand = rand_int % num;
    return key_rand;
  }

  @Override
  public void init(final Properties p) throws WorkloadException {
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
    keyrange_num = Integer.valueOf(p.getProperty(KEYRANGE_NUM_PROPERTY, KEYRANGE_NUM_DEFAULT));

    key_dist_a = Double.valueOf(p.getProperty(KEY_DIST_A_PROPERTY, KEY_DIST_A_DEFAULT));
    key_dist_b = Double.valueOf(p.getProperty(KEY_DIST_B_PROPERTY, KEY_DIST_B_DEFAULT));

    Status s;
    if (value_max > Long.valueOf(p.getProperty(MIX_MAX_VALUE_SIZE_PROPERTY, MIX_MAX_VALUE_SIZE_DEFAULT))) {
      value_max = Long.valueOf(p.getProperty(MIX_MAX_VALUE_SIZE_PROPERTY, MIX_MAX_VALUE_SIZE_DEFAULT));
    }


    //TODO: check if we need a ReadOption here, refer to the TimeSeriesWorkload.
    char[] value_buffer;
    QueryDecider query = new QueryDecider();

    query.Initiate(ratio);
    // the limit of qps initiation
    // TODO: discuss whether we need RateLimiter or not

    createMixGraph();

    keysequence = new MixGraphGenerator(num, query, key_dist_a, key_dist_b,
        keyrange_dist_a, keyrange_dist_b, keyrange_dist_c, keyrange_dist_d, keyrange_num);
    // initiate finished


  }


  public void createMixGraph() {
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    // TODO: replace this keysequence generation function to mixgraph workload
    String dbkey = keysequence.nextValue();

//    int keynum = keysequence.nextValue().nextInt();
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

}
