package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class CoalesceHiveUDF extends UDF {

  public static String evaluate(String input, Object... defaults) {
    return (String) Utils.coalesce(defaults);
  }
}
