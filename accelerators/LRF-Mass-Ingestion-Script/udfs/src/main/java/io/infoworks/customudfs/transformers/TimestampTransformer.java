package io.infoworks.customers.att.transformers;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.function.Function;

public class TimestampTransformer implements Function<Timestamp, Timestamp>, Serializable {

  @Override
  public Timestamp apply(Timestamp untransformed) {
    return new Timestamp(System.currentTimeMillis());
  }
}
