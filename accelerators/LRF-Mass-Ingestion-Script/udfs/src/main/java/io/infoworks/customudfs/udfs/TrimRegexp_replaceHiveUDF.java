package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.datanucleus.util.StringUtils;

public class TrimRegexp_replaceHiveUDF extends UDF {

  public static String evaluate(String unTransformed, String param1, String param2) {
    if (unTransformed == null) {
      return null;
    }

    if (StringUtils.isEmpty(unTransformed)) {
      return null;
    }
    String transformed = unTransformed.replaceAll(param1, param2);
    return transformed;
  }
}
