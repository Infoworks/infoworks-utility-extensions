package io.infoworks.customudfs.udfs;

import java.text.ParseException;
import org.apache.hadoop.hive.ql.exec.UDF;

public class ReturnZeroIfParamsEqualHiveUDF extends UDF {

  public static Integer evaluate(Integer input, String secondInteger, String dateformat)
      throws ParseException {
    if (input == null) {
      return 0;
    }
    if (input.equals(secondInteger)) {
      return 0;
    }

    return input;
  }
}
