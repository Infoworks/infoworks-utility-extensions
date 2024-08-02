package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NvlLongHiveUDF extends UDF {

  public static Long evaluate(String input, Long defaultInput) {
    if (input != null) {
      return Long.valueOf(input);
    }
    return defaultInput;
  }
}
