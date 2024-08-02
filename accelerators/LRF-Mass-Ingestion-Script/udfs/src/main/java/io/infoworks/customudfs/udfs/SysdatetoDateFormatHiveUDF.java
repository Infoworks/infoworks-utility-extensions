package io.infoworks.customudfs.udfs;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.hadoop.hive.ql.exec.UDF;

public class SysdatetoDateFormatHiveUDF extends UDF {

  public static Date evaluate(Date input) {
    return new java.sql.Date(Calendar.getInstance().getTime().getTime());
  }

  public static String evaluate(String input) {
    SimpleDateFormat parseFormat = new SimpleDateFormat("yyyy-MM-dd");
    Date date = new java.sql.Date(Calendar.getInstance().getTime().getTime());
    return parseFormat.format(date);
  }
}
