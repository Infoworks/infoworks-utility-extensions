package io.infoworks.customudfs.udfs;

import java.sql.Date;
import java.sql.Timestamp;
import org.apache.hadoop.hive.ql.exec.UDF;

public class TimestampTransformerHiveUDF extends UDF {

  public static Timestamp evaluate(Timestamp untransformed) {
    return new Timestamp(System.currentTimeMillis());
  }

  public static Timestamp evaluate(Date untransformed) {
    return new Timestamp(System.currentTimeMillis());
  }
}
