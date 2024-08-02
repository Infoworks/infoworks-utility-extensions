package io.infoworks.customudfs.udfs;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CastStringToTimestampFormatHiveUDF extends UDF {

  public static Timestamp evaluate(String input, String timestampformat) throws ParseException {
    if (StringUtils.isEmpty(input)) {
      return null;
    }

    // Timestamp parsedtimestamp = Timestamp.valueOf(input);

    SimpleDateFormat parseFormat = new SimpleDateFormat(timestampformat);
    return new java.sql.Timestamp(parseFormat.parse(input).getTime());
  }
}
