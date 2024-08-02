package io.infoworks.customudfs.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CaseWhenIsNullThenEmpty extends UDF {
  public static String evaluate(String input, String secondString) {
    if (StringUtils.isEmpty(input)) {
      return null;
    }
    return input;
  }
}
