package io.infoworks.customers.att.transformers;

import java.io.Serializable;
import java.util.function.Function;

public class NonASCIICleanup implements Function<String, String>, Serializable {

  @Override
  public String apply(String unTransformed) {
    if (unTransformed == null) {
      return null;
    }
    String transformed = unTransformed.replaceAll("[^\\x00-\\x7F]", "");
    return transformed;
  }

  public static void main(String[] args) {
    NonASCIICleanup transformer = new NonASCIICleanup();
    String transformed = transformer.apply("jå∫∆avµa2bl√øog");
    System.out.println(transformed);
  }
}
