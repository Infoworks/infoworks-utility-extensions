package io.infoworks.customudfs.udfs;

import static org.apache.commons.lang.StringUtils.length;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class SubstrCharlengthTrimTrailHiveUDF extends UDF {

  public static String evaluate(String input, int length) {
    if (StringUtils.isEmpty(input)) {
      return null;
    }

    int len = input.length();
    for (; len > 0; len--) {
      if (!Character.isWhitespace(input.charAt(len - 1))) break;
    }
    input = input.substring(0, len);
    System.out.println(length(input));
    return (input.substring(input.length() - length));
  }
}
