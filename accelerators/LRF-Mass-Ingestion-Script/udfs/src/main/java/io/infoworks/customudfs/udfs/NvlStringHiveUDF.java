package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NvlStringHiveUDF extends UDF {

  public static String evaluate(String input, String defaultInput) {
    if (input != null) {
      return input;
    }
    return defaultInput;
  }
}
