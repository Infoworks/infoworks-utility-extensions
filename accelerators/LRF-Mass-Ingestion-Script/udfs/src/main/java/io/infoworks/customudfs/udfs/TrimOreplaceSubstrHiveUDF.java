package io.infoworks.customudfs.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class TrimOreplaceSubstrHiveUDF extends UDF {

  public static String evaluate(
      String input, Integer beginIndexForSubstring, String searchString, String replaceString) {
    if (StringUtils.isEmpty(input)) {
      return input;
    }
    input = input.substring(beginIndexForSubstring);
    input = input.replace(searchString, replaceString);
    input = input.trim();
    return input;
  }
}
