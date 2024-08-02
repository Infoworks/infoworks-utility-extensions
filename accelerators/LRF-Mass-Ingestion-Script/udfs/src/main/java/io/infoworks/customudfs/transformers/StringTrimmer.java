package io.infoworks.customers.att.transformers;

import java.io.Serializable;
import java.util.function.Function;

public class StringTrimmer implements Function<String, String>, Serializable {

  @Override
  public String apply(String unTrimmed) {
    if (unTrimmed == null) {
      return null;
    }
    String trimmed = unTrimmed.trim();
    return trimmed;
  }
}
