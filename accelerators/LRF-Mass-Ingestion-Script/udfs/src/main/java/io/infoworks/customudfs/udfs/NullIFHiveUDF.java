package io.infoworks.customudfs.udfs;

import java.sql.Date;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

public class NullIFHiveUDF extends UDF {

  public static String evaluate(String input, String secondexpression) {
    if (StringUtils.isEmpty(input)) {
      return null;
    }
    if (input.equals(secondexpression)) {
      return null;
    }

    return input;
  }

  public static Long evaluate(Long input, String secondExpression) {
    if (String.valueOf(input).equals(String.valueOf(secondExpression))) {
      return null;
    }
    return input;
  }

  public static Integer evaluate(Integer input, String secondExpression) {
    if (String.valueOf(input).equals(String.valueOf(secondExpression))) {
      return null;
    }
    return input;
  }

  public static Date evaluate(Date input, String secondExpression) {
    if (String.valueOf(input).equals(secondExpression)) {
      return null;
    }
    return input;
  }
}
