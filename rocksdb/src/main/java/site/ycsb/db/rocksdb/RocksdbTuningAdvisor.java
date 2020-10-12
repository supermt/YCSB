package site.ycsb.db.rocksdb;

import site.ycsb.TuningAdvisor;

public class RocksdbTuningAdvisor extends TuningAdvisor {
  @Override
  public void setUpDefaultOptions() {
    System.out.println("Bootstrap the default options");
  }
}
