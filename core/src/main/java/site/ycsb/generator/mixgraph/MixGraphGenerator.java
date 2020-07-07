package site.ycsb.generator.mixgraph;

import site.ycsb.ByteIterator;
import site.ycsb.RandomByteIterator;
import site.ycsb.generator.Generator;
import site.ycsb.generator.NumberGenerator;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

enum DistributionType {
  kFixed,
  kUniform,
  kNormal
}

public class MixGraphGenerator extends Generator<MixGraphKey> {
  private final RandomModels model;
  private final int prefix_size_;
  private final int key_size_;
  protected NumberGenerator fieldlengthgenerator;
  double a, b;
  long num;
  GenerateTwoTermExpKeys gen_exp;
  boolean use_prefix_modeling = false;
  boolean use_random_modeling = false;
  private int keys_per_prefix_ = 0;


  public MixGraphGenerator(long num,
                           double key_dist_a, double key_dist_b,
                           double keyrange_dist_a, double keyrange_dist_b,
                           double keyrange_dist_c, double keyrange_dist_d,
                           long keyrange_num, int key_size, int keys_per_prefix, int prefix_size) {
    super();
    this.num = num;
    this.a = key_dist_a;
    this.b = key_dist_b;
    this.key_size_ = key_size;
    this.keys_per_prefix_ = keys_per_prefix;
    this.prefix_size_ = prefix_size;

    gen_exp = new GenerateTwoTermExpKeys(num);
    if (keyrange_dist_a != 0.0 || keyrange_dist_b != 0.0 ||
        keyrange_dist_c != 0.0 || keyrange_dist_d != 0.0) {
      use_prefix_modeling = true;
      gen_exp.InitiateExpDistribution(
          num, keyrange_dist_a, keyrange_dist_b,
          keyrange_dist_c, keyrange_dist_d, keyrange_num);
    }
    if (key_dist_a == 0 || key_dist_b == 0) {
      use_random_modeling = true;
    }

    if (use_random_modeling) {
      model = RandomModels.kRandomModel;
    } else if (use_prefix_modeling) {
      model = RandomModels.kPrefixRandomModel;
    } else {
      model = RandomModels.kPowerCDFRandomModel;
    }

  }

  // Generate key according to the given specification and random number.
  // The resulting key will have the following format (if keys_per_prefix_
  // is positive), extra trailing bytes are either cut off or padded with '0'.
  // The prefix value is derived from key value.
  //   ----------------------------
  //   | prefix 00000 | key 00000 |
  //   ----------------------------
  // If keys_per_prefix_ is 0, the key is simply a binary representation of
  // random number followed by trailing '0's
  //   ----------------------------
  //   |        key 00000         |
  //   ----------------------------
  String GenerateKeyFromInt(long v, long num_keys) {
    StringBuilder key = new StringBuilder();
    // TODO: learn how to generate the key string by a number
    if (keys_per_prefix_ > 0) {
      long num_prefix = num_keys / keys_per_prefix_;
      long prefix = v % num_prefix;
      int bytes_to_fill = Math.min(prefix_size_, 8);
      // java uses big endian
      key.append(String.valueOf(prefix), 0, bytes_to_fill);
      while (prefix_size_ > 8) {
        key.append('0');
      }
    }

    int bytes_to_fill = Math.min(key_size_ - key.length(), 8);

    key.append(String.valueOf(v), 0, bytes_to_fill);
    while (key_size_ > key.length()) {
      key.append('0');
    }

    return key.toString();
  }

  private long PowerCdfInversion(double u, double a, double b) {
    // TODO: generate PowerCDFInversion from C++
    double ret;

    ret = Math.pow((u / a), (1 / b));
    return (long) (Math.ceil(ret));
  }

  HashMap<String, ByteIterator> RandomString(String key, List<String> fieldnames) {
    HashMap<String, ByteIterator> values = new HashMap<>();

    for (String fieldkey : fieldnames) {
      ByteIterator data;
      // fill with random data
      data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());

      values.put(fieldkey, data);
    }
    return values;
  }

  @Override
  public MixGraphKey nextValue() {
    long ini_rand, rand_v, key_rand, key_seed = 0;
    ini_rand = fieldlengthgenerator.nextValue().longValue();
    rand_v = ini_rand % num;

    double u = (double) (rand_v) / num;
    // Generate the keyID based on the key hotness and prefix hotness
    switch (model) {
      case kRandomModel:
        key_rand = ini_rand;
        break;
      case kPrefixRandomModel:
        key_rand =
            gen_exp.DistGetKeyID(ini_rand, a, b);
        break;
      case kPowerCDFRandomModel:
      default:
        key_seed = PowerCdfInversion(u, a, b);
        key_rand = (new Random(key_seed).nextLong()) % num;
    }

    return new MixGraphKey(ini_rand, rand_v, key_rand, key_seed, GenerateKeyFromInt(key_rand, num));
  }

  @Override
  public MixGraphKey lastValue() {
    return null;
  }


}
//
//class RandomGenerator {
//  StringBuilder data_;
//  int pos_;
//  BaseDistribution dist_;
//
//  public RandomGenerator(Properties p,List<String> fieldnames) {
//    long max_value_size = Long.valueOf(p.getProperty(VALUE_SIZE_MAX_PROPERTY, VALUE_SIZE_MAX_DEFAULT));
//    long min_value_size = Long.valueOf(p.getProperty(VALUE_SIZE_MIN_PROPERTY, VALUE_SIZE_MIN_DEFAULT)),
////      Long.valueOf(p.getProperty(VALUE_SIZE_MAX_PROPERTY, VALUE_SIZE_MAX_DEFAULT));
//    DistributionType distributionType = DistributionType.valueOf(p.getProperty(
//        VALUE_SIZE_DISTRIBUTION_TYPE_E_PROPERTY, VALUE_SIZE_DISTRIBUTION_TYPE_E_DEFAULT));
//    switch (distributionType) {
//      case kUniform:
//        dist_ = new UniformDistribution(min_value_size, max_value_size);
//        break;
//      case kNormal:
//        dist_ = new NormalDistribution(min_value_size, max_value_size);
//        break;
//      case kFixed:
//      default:
//        long value_size = Long.valueOf(p.getProperty(VALUE_SIZE_PROPERTY, VALUE_SIZE_DEFAULT));
//        dist_ = new FixedDistribution(value_size);
//        max_value_size = value_size;
//    }
//    // We use a limited amount of data over and over again and ensure
//    // that it is larger than the compression window (32KB), and also
//    // large enough to serve all typical value sizes we want to write.
//    Random rnd = new Random(301);
//    String piece;
//    while (data_.length() < Math.max(1048576, max_value_size)) {
//      // Add a short fragment that is as compressible as specified
//      // by FLAGS_compression_ratio.
//      // TODO: generate random string in piece
////        piece = RandomString();
//      data_.append(piece);
//    }
//    pos_ = 0;
//  }
//}