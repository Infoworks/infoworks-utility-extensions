package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class CoalesceTrimHiveUDF extends UDF {

  public static String evaluate(String input, Object... defaults) {
    if (input != null) {
      input = input.trim();
      return input;
    }
    return (String) Utils.coalesce(defaults);
  }
}
