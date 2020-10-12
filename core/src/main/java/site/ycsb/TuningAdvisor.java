package site.ycsb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class TuningAdvisor {
  private final Object option_object;
  /**
   * possible options limit the target options into optional values.
   * for now we lists out all possible values.
   * //TODO: we use the Type object to determine the value space.
   */
  private Map<String, List<Class>> possible_options;
  private Map<String, Class> option_types;
  private Properties properties;

  public TuningAdvisor() {
    option_object = null;
    option_types = new HashMap<>();
    option_types.put("IntOption1", Integer.class);
  }

  public TuningAdvisor(Object options, Map<String, Class> option_types, int option_type) {
    this.option_object = options;
    this.option_types.putAll(option_types);
  }

  public TuningAdvisor(Object options, Map<String, List<Class>> possible_options) {
    this.option_object = options;
    this.possible_options = possible_options;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  public abstract void setUpDefaultOptions();

}
