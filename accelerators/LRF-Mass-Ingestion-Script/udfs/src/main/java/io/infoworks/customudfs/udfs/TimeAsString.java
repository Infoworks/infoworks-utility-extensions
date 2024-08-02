package io.infoworks.customudfs.udfs;

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.hive.ql.exec.UDF;

public class TimeAsString extends UDF {

  public static String evaluate(String input) throws ParseException {
    return new Timestamp(System.currentTimeMillis())
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("HH:mm:ss"));
  }
}
