package io.infoworks.customudfs.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class NvlIntegerHiveUDF extends UDF {

  public static int evaluate(String input, String defaultInput) {
    if (!StringUtils.isEmpty(input)) {
      return Integer.parseInt(input);
    }
    return Integer.parseInt(defaultInput);
  }
}
