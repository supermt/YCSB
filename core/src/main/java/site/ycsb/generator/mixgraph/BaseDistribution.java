package site.ycsb.generator.mixgraph;

import java.util.Random;

// A generator base class
public abstract class BaseDistribution extends Random {
  private final long min_value_size_;
  private final long max_value_size_;

  public BaseDistribution(long _min, long _max) {
    min_value_size_ = _min;
    max_value_size_ = _max;
  }

  public long Generate() {
    long val = Get();
    if (NeedTruncate()) {
      val = Math.max(min_value_size_, val);
      val = Math.min(max_value_size_, val);
    }
    return 0;
  }

  private boolean NeedTruncate() {
    return true;
  }

  protected abstract long Get();
}

class UniformDistribution extends BaseDistribution {
  public UniformDistribution(long _min, long _max) {
    super(_min, _max);
  }

  @Override
  protected long Get() {
    return 1;
  }

}

class NormalDistribution extends BaseDistribution {

  public NormalDistribution(long _min, long _max) {
    super(_min, _max);
  }

  @Override
  protected long Get() {
    return 0;
  }
}

class FixedDistribution extends BaseDistribution {

  public FixedDistribution(long value_size) {
    super(value_size, value_size);
  }

  @Override
  protected long Get() {
    return 0;
  }
}