package io.infoworks.customudfs.udfs;

import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class StringtoDateFormatHiveUDF extends UDF {
  public static Date evaluate(String input, String defaultConcatvalue, String dateformat)
      throws ParseException {

    if (StringUtils.isEmpty(input)) {

      return null;
    }
    input = defaultConcatvalue + input;
    DateFormat dateFormat = new SimpleDateFormat(dateformat);
    return Date.valueOf(LocalDate.parse(input, DateTimeFormatter.ofPattern(dateformat)));
  }
}
