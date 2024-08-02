package io.infoworks.customudfs.udfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class SubstrHiveUDF extends UDF {

  public static String evaluate(String input, int startIndex, int endIndex) {
    if (StringUtils.isEmpty(input)) {
      return input;
    }

    try {
      return input.substring(startIndex, endIndex);
    } catch (Exception e) {

      return null;
    }
  }
}
