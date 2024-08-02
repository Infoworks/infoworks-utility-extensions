package io.infoworks.customers.att.transformers;

import java.io.Serializable;
import java.util.function.Function;

public class DecimalCleanup implements Function<String, Double>, Serializable {

  @Override
  public Double apply(String unTransformed) {
    if (unTransformed == null) {
      return null;
    }
    if (unTransformed.trim().contains(" ")) {
      return -1.0;
    }
    if (unTransformed.matches("[a-zA-Z]+")) {
      return -1.0;
    }
    return Double.parseDouble(unTransformed);
  }
}
