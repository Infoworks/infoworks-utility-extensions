package io.infoworks.customudfs.udfs;

import java.sql.Timestamp;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CoalesceCastToTimestamp0HiveUDF extends UDF {

  public static Timestamp evaluate(String input, Object... defaults) {

    if (StringUtils.isEmpty(input)) {
      return null;
    }

    Timestamp parsedtimestamp = Timestamp.valueOf(input);

    return Utils.coalesce(parsedtimestamp);
  }
}
