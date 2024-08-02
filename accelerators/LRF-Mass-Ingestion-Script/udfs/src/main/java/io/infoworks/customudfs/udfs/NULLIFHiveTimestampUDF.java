package io.infoworks.customudfs.udfs;

import java.sql.Timestamp;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class NULLIFHiveTimestampUDF extends UDF {

  public static Timestamp evaluate(Timestamp input, Timestamp secondexpression) {
    if (StringUtils.isEmpty(String.valueOf(input))) {
      return null;
    }
    if (String.valueOf(input).equals(String.valueOf(secondexpression))) {
      return null;
    }

    return input;

    // Test
  }
}
