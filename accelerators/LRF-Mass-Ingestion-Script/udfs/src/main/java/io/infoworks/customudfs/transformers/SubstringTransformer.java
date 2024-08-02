package io.infoworks.customers.att.transformers;

import java.io.Serializable;
import java.util.function.Function;

public class SubstringTransformer implements Function<String, String>, Serializable {

  @Override
  public String apply(String unTransformed) {
    if (unTransformed == null) {
      return null;
    }
    int startIndex = 1;
    int endIndex = Math.min(unTransformed.length() - 1, 19);
    String transformed = unTransformed.substring(startIndex, endIndex);
    return transformed;
  }
}
