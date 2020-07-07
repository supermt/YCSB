package com.yahoo.ycsb.generator.mixgraph;

import com.yahoo.ycsb.Status;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.Vector;

class KeyrangeUnit {
  public long keyrange_start;
  public long keyrange_access;
  public long keyrange_keys;
}

//class Random64 {
//  private MT19937_64 generator_;
//
//  public Random64(long s) {
//    this.generator_ = new MT19937_64(s);
//  }
//
//  // Generates the next random number
//  long Next() {
//    return generator_.next(64 - 1); // There is no 64-bits unsigned long, generate 63 instead
//  }
//}

public class GenerateTwoTermExpKeys {
  public long keyrange_rand_max_;
  public long keyrange_size_;
  public long keyrange_num_;
  public boolean initiated_;
  public Vector<KeyrangeUnit> keyrange_set_;

  public GenerateTwoTermExpKeys(long FLAGS_num) {
    keyrange_rand_max_ = FLAGS_num;
    keyrange_set_ = new Vector<>();
    initiated_ = false;
  }

  public static void swap(int i, int j, Vector<KeyrangeUnit> arr) {
    KeyrangeUnit t = arr.get(i);
    arr.set(i, arr.get(j));
    arr.set(j, t);
  }

  // Initiate the KeyrangeUnit vector and calculate the size of each
  // KeyrangeUnit.
  public Status InitiateExpDistribution(long total_keys, double prefix_a,
                                        double prefix_b, double prefix_c,
                                        double prefix_d, long keyrange_num) {
    long amplify = 0;
    long keyrange_start = 0;
    initiated_ = true;
    if (keyrange_num <= 0) {
      keyrange_num_ = 1;
    } else {
      keyrange_num_ = keyrange_num;
    }
    keyrange_size_ = total_keys / keyrange_num_;

    // Calculate the key-range shares size based on the input parameters
    for (long pfx = keyrange_num_; pfx >= 1; pfx--) {
      // Step 1. Calculate the probability that this key range will be
      // accessed in a query. It is based on the two-term expoential
      // distribution
      double keyrange_p = prefix_a * Math.exp(prefix_b * pfx) +
          prefix_c * Math.exp(prefix_d * pfx);
      if (keyrange_p < Math.pow(10.0, -16.0)) {
        keyrange_p = 0.0;
      }
      // Step 2. Calculate the amplify
      // In order to allocate a query to a key-range based on the random
      // number generated for this query, we need to extend the probability
      // of each key range from [0,1] to [0, amplify]. Amplify is calculated
      // by 1/(smallest key-range probability). In this way, we ensure that
      // all key-ranges are assigned with an Integer that  >=0
      if (amplify == 0 && keyrange_p > 0) {
        amplify = (long) (Math.floor(1 / keyrange_p)) + 1;
      }

      // Step 3. For each key-range, we calculate its position in the
      // [0, amplify] range, including the start, the size (keyrange_access)
      KeyrangeUnit p_unit = new KeyrangeUnit();
      p_unit.keyrange_start = keyrange_start;
      if (0.0 >= keyrange_p) {
        p_unit.keyrange_access = 0;
      } else {
        p_unit.keyrange_access =
            (long) (Math.floor(amplify * keyrange_p));
      }
      p_unit.keyrange_keys = keyrange_size_;
      keyrange_set_.add(p_unit);
      keyrange_start += p_unit.keyrange_access;
    }
    keyrange_rand_max_ = keyrange_start;

    // Step 4. Shuffle the key-ranges randomly
    // Since the access probability is calculated from small to large,
    // If we do not re-allocate them, hot key-ranges are always at the end
    // and cold key-ranges are at the begin of the key space. Therefore, the
    // key-ranges are shuffled and the rand seed is only decide by the
    // key-range hotness distribution. With the same distribution parameters
    // the shuffle results are the same.
//    Random64 rand_loca = new Random64(keyrange_rand_max_);
//    for (int i = 0; i < keyrange_num; i++) {
//      int pos = (int) (rand_loca.Next() % keyrange_num);
//      assert (i >= 0 && i < (long) (keyrange_set_.size()) &&
//          pos >= 0 && pos < (long) (keyrange_set_.size()));
//      swap(i, pos, keyrange_set_);
//    }
    Collections.shuffle(keyrange_set_);


    // Step 5. Recalculate the prefix start postion after shuffling
    long offset = 0;
    for (KeyrangeUnit p_unit : keyrange_set_) {
      p_unit.keyrange_start = offset;
      offset += p_unit.keyrange_access;
    }

    return Status.OK;
  }

  // Generate the Key ID according to the input ini_rand and key distribution
  long DistGetKeyID(long ini_rand, double key_dist_a,
                    double key_dist_b) {
    long keyrange_rand = ini_rand % keyrange_rand_max_;

    // Calculate and select one key-range that contains the new key
    int start = 0, end = keyrange_set_.size();
    while (start + 1 < end) {
      int mid = start + (end - start) / 2;
      assert (mid >= 0 && mid < (long) (keyrange_set_.size()));
      if (keyrange_rand < keyrange_set_.get(mid).keyrange_start) {
        end = mid;
      } else {
        start = mid;
      }
    }
    long keyrange_id = start;

    // Select one key in the key-range and compose the keyID
    long key_offset, key_seed;
    if (key_dist_a == 0.0 || key_dist_b == 0.0) {
      key_offset = ini_rand % keyrange_size_;
    } else {
      double u =
          (double) (ini_rand % keyrange_size_) / keyrange_size_;
      key_seed = (long) (
          Math.ceil(Math.pow((u / key_dist_a), (1 / key_dist_b))));
      Random64 rand_key = new Random64(key_seed);
      key_offset = rand_key.Next() % keyrange_size_;
    }
    return keyrange_size_ * keyrange_id + key_offset;
  }
}

