package com.yahoo.ycsb.generator.mixgraph;

import java.util.Random;

public class Random64 {
  private Random generator_;

  public Random64(long s) {
    this.generator_ = new Random(s);
  }

  // Generates the next random number
  public long Next() {
    return generator_.nextLong(); // There is no 64-bits unsigned long, generate 63 instead
  }
}
