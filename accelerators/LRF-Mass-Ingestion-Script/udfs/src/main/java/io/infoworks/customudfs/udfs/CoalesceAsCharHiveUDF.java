package io.infoworks.customudfs.udfs;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CoalesceAsCharHiveUDF extends UDF {

  public static String evaluate(String input, Integer size, String defaultInput) {
    String coalescedObject = Utils.coalesce(input, defaultInput);
    if (StringUtils.isEmpty(coalescedObject)) {
      return coalescedObject;
    }
    return coalescedObject.substring(0, size);
  }
}
