package io.infoworks.customudfs.udfs;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class StringToTimestampHiveUDF extends UDF {

  public static Timestamp evaluate(String input) throws ParseException {
    if (StringUtils.isEmpty(input)) {
      return null;
    }
    // 09/03/2022 22:34:31

    try {
      String timestampString =
          input.substring(6, 10)
              + "-"
              + input.substring(0, 2)
              + "-"
              + input.substring(3, 5)
              + input.substring(10, 19);

      SimpleDateFormat parseFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      return new java.sql.Timestamp(parseFormat.parse(timestampString).getTime());
    } catch (Exception e) {

      return null;
    }
  }
}
