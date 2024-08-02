package io.infoworks.customudfs.udfs;

import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CastDateFormatElseAnotherDateHiveUDF extends UDF {
  public static Date evaluate(
      String input, String secondString, String secondDate, String dateformat)
      throws ParseException {

    DateFormat dateFormat = new SimpleDateFormat(dateformat);

    if (StringUtils.isEmpty(input)) {
      return null;
    }

    if (!input.equals(secondString)) {
      return Date.valueOf(LocalDate.parse(input, DateTimeFormatter.ofPattern(dateformat)));
    }

    return Date.valueOf(LocalDate.parse(secondDate, DateTimeFormatter.ofPattern(dateformat)));
  }
}
