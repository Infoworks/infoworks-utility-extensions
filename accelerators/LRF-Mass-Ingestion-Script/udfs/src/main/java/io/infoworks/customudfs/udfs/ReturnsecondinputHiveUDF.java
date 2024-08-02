package io.infoworks.customudfs.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ReturnsecondinputHiveUDF extends UDF {

  public static String evaluate(String input, String secondinput) {
    return secondinput;
  }

  public static String evaluate(int input, String secondinput) {
    return secondinput;
  }
}
