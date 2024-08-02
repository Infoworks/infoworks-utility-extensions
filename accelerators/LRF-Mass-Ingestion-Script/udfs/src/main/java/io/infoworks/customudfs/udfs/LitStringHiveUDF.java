package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class LitStringHiveUDF extends UDF {

  public static String evaluate(String input) {
    return input;
  }
}
