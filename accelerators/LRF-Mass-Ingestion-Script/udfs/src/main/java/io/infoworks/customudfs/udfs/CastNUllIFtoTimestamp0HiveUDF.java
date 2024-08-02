package io.infoworks.customudfs.udfs;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CastNUllIFtoTimestamp0HiveUDF extends UDF {

  public static Timestamp evaluate(String input, String secondexpression, String timestampformat)
      throws ParseException {

    if (StringUtils.isEmpty(input)) {
      return null;
    }

    try {
      SimpleDateFormat parseFormat = new SimpleDateFormat(timestampformat);
      return new java.sql.Timestamp(
          parseFormat.parse(NullIFHiveUDF.evaluate(input, secondexpression)).getTime());

    } catch (Exception e) {

      return null;
    }
  }
}
