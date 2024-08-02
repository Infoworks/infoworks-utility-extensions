package io.infoworks.customudfs.udfs;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CastReturnCurrentTimestampIfNullElseGivenTimestampHiveUDF extends UDF {

  public static Timestamp evaluate(String input) throws ParseException {

    SimpleDateFormat parseFormat = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss.SSSSSS");
    if (StringUtils.isEmpty(input)) {
      Date date = new Date();
      return new Timestamp(date.getTime());
    }

    return new java.sql.Timestamp(parseFormat.parse(input).getTime());
  }
}
