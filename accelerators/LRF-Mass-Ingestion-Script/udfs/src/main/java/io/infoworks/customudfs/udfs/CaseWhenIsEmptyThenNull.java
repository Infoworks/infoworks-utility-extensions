package io.infoworks.customudfs.udfs;

import java.sql.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CaseWhenIsEmptyThenNull extends UDF {
  public static Date evaluate(String input) {
    if (StringUtils.isEmpty(input)) {
      return null;
    }
    return Date.valueOf(input);
  }
}
