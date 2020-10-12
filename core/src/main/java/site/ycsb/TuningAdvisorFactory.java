package site.ycsb;

import org.apache.htrace.core.Tracer;

import java.util.Properties;

public class TuningAdvisorFactory {
  private TuningAdvisorFactory() {
    // not used
  }

  public static TuningAdvisor newTA(String advisor_name, Properties properties) throws UnknownDBException {
    ClassLoader classLoader = DBFactory.class.getClassLoader();

    TuningAdvisor ret;

    try {
      Class taclass = classLoader.loadClass(advisor_name);

      ret = (TuningAdvisor) taclass.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    ret.setProperties(properties);

    return ret;
  }

}
