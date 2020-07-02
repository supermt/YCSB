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

import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.*;

import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

/**
 * this workload is inspired by the paper "Characterizing, Modeling,
 * and Benchmarking RocksDB Key-Value Workloads at Facebook",
 * https://www.usenix.org/conference/fast20/presentation/cao-zhichao
 * the logic code is inspired by RocksDB.
 */

public class MixGraphWorkload extends Workload {
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
  public static final String KEYRANGE_DIST_B_PROPERTY = "keyrange_dist_b";
  public static final String KEYRANGE_DIST_C_PROPERTY = "keyrange_dist_c";
  public static final String KEYRANGE_DIST_D_PROPERTY = "keyrange_dist_d";

  public static final String KEYRANGE_NUM_PROPERTY = "keyrange_num";
  public static final String KEYRANGE_NUM_DEFAULT = "1";


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


  final long default_value_max = 1024 * 1024;


  @Override
  public void init(final Properties p) throws WorkloadException {
    long read = 0;  // including single gets and Next of iterators
    long gets = 0;
    long puts = 0;
    long found = 0;
    long seek = 0;
    long seek_found = 0;
    long bytes = 0;
    long value_max = default_value_max;
    long scan_len_max = Long.valueOf(p.getProperty(MIX_MAX_SCAN_LEN_PROPERTY, MIX_MAX_SCAN_LEN_DEFAULT));
    double write_rate = 1000000.0;
    double read_rate = 1000000.0;
    boolean use_prefix_modeling = false;
    boolean use_random_modeling = false;
    GenerateTwoTermExpKeys gen_exp = new GenerateTwoTermExpKeys(Long.valueOf(p.getProperty(NUM_PROPERTY, NUM_DEFAULT)));
    Vector<Double> ratio = new Vector<>();

    ratio.add(Double.valueOf(p.getProperty(MIX_GET_RATIO_PROPERTY, MIX_GET_RARIO_DEFAULT)));
    ratio.add(Double.valueOf(p.getProperty(MIX_PUT_RATIO_PROPERTY, MIX_PUT_RATIO_DEFAULT)));
    ratio.add(Double.valueOf(p.getProperty(MIX_SEEK_RATIO_PROPERTY, MIX_SEEK_RATIO_DEFAULT)));

    //TODO: check if we need a ReadOption here, refer to the TimeSeriesWorkload.

    createMixGraph();
  }


  public void createMixGraph() {
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return false;
  }


}
