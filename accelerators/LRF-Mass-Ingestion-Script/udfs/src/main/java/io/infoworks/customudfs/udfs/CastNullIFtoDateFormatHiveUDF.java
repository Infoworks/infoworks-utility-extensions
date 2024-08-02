package io.infoworks.customudfs.udfs;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class CastNullIFtoDateFormatHiveUDF extends UDF {
  public static Date evaluate(String input, String secondexpression, String dateformat) {

    if (StringUtils.isEmpty(input)) {
      return null;
    }

    try {

      DateFormat dateFormat = new SimpleDateFormat(dateformat);
      // System.out.println(Date.valueOf(LocalDate.parse(NullIFHiveUDF.evaluate(input,
      // secondexpression), DateTimeFormatter.ofPattern(dateformat))).getClass());

      return Date.valueOf(
          LocalDate.parse(
              NullIFHiveUDF.evaluate(input, secondexpression),
              DateTimeFormatter.ofPattern(dateformat)));
    } catch (Exception e) {

      return null;
    }
  }
}
