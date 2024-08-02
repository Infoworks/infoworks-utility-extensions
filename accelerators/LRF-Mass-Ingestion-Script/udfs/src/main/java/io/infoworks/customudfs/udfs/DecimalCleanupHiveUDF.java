package io.infoworks.customudfs.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DecimalCleanupHiveUDF extends UDF {

  public static Double evaluate(String unTransformed) {
    if (unTransformed == null) {
      return null;
    }
    // added this check to see if the string is empty and then return null

    if (StringUtils.isEmpty(unTransformed)) {
      return null;
    }
    if (unTransformed.trim().contains(" ")) {
      return -1.0;
    }
    if (unTransformed.matches("[a-zA-Z]+")) {
      return -1.0;
    }
    return Double.parseDouble(unTransformed);
  }
}
