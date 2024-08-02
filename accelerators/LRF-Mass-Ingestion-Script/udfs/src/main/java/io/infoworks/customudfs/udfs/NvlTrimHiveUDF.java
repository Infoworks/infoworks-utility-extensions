package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NvlTrimHiveUDF extends UDF {

  public static String evaluate(String input, String defaultInput) {
    if (input != null) {
      input = input.trim();
      return input;
    }
    return defaultInput;
  }
}
