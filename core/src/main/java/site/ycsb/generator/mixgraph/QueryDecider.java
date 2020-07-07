package site.ycsb.generator.mixgraph;

import site.ycsb.Status;

import java.util.Vector;

public class QueryDecider {
  Vector<Integer> type_;
  Vector<Double> ratio_;
  int range_;

  public QueryDecider() {
  }


  public Status Initiate(Vector<Double> ratio_input) {
    int range_max = 1000;
    double sum = 0.0;
    type_ = new Vector<>();
    ratio_ = new Vector<>();

    for (Double ratio : ratio_input) {
      sum += ratio;
    }
    range_ = 0;
    for (Double ratio : ratio_input) {
      range_ += Math.ceil(range_max * (ratio / sum));
      type_.add(range_);
      ratio_.add(ratio / sum);
    }
    return Status.OK;
  }

  public int GetType(long rand_num) {
    if (rand_num < 0) {
      rand_num = rand_num * (-1);
    }
    assert (range_ != 0);
    int pos = (int) (rand_num % range_);
    for (int i = 0; i < type_.size(); i++) {
      if (pos < type_.get(i)) {
        return i;
      }
    }
    return 0;
  }
}

