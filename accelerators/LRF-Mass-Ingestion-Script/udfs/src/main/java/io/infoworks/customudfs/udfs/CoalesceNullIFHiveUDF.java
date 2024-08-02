package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class CoalesceNullIFHiveUDF extends UDF {

  public static Long evaluate(String input, String secondExpression, Long defaultInput) {
    try {
      return Long.parseLong(
          Utils.coalesce(NullIFHiveUDF.evaluate(input, secondExpression), defaultInput.toString()));
    } catch (Exception e) {
      return 0L;
    }
  }

  public static Double evaluate(String input, String secondExpression, Double defaultInput) {
    try {
      return Double.parseDouble(
          Utils.coalesce(NullIFHiveUDF.evaluate(input, secondExpression), defaultInput.toString()));
    } catch (Exception e) {
      return 0.0;
    }
  }

  public static Integer evaluate(String input, String secondExpression, Integer defaultInput) {
    try {
      return Integer.parseInt(
          Utils.coalesce(NullIFHiveUDF.evaluate(input, secondExpression), defaultInput.toString()));
    } catch (Exception e) {
      return 0;
    }
  }

  public static String evaluate(String input, String secondExpression, String defaultInput) {
    return Utils.coalesce(NullIFHiveUDF.evaluate(input, secondExpression), defaultInput);
  }
}
