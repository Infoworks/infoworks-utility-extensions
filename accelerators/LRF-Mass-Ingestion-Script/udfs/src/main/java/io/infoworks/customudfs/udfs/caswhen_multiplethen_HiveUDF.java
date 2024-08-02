package io.infoworks.customudfs.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class caswhen_multiplethen_HiveUDF extends UDF {

  public static String evaluate(String input) {
    if (StringUtils.isEmpty(input)) {
      return null;
    } else if (input.equals("Q")) {

      return "IL";

    } else if (input.equals("M")) {
      return "MI";
    } else if (input.equals("G")) {
      return "IN";
    } else if (input.equals("J")) {
      return "OH";
    } else if (input.equals("E")) {
      return "WI";
    }
    return "??";
  }
}
