package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class LitIntegerHiveUDF extends UDF {

  public static Integer evaluate(Integer input) {
    return input;
  }

  public static Double evaluate(Double input) {
    return input;
  }

  public static String evaluate(String input) {
    return input;
  }

  public static Float evaluate(Float input) {
    return input;
  }

  public static Long evaluate(Long input) {
    return input;
  }
}
