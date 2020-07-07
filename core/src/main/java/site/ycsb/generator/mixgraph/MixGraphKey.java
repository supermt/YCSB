package com.yahoo.ycsb.generator.mixgraph;

public class MixGraphKey {
  private final String key;
  public long ini_rand, rand_v, key_rand, key_seed;

  public MixGraphKey(long ini_rand, long rand_v, long key_rand, long key_seed,String key) {
    this.ini_rand = ini_rand;
    this.rand_v = rand_v;
    this.key_rand = key_rand;
    this.key_seed = key_seed;
    this.key = key;
  }

  public String getKeyString() {
    return this.key;
  }
}
