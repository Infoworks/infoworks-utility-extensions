package io.infoworks.customudfs.udfs;

public class Utils {

  public static <T> T coalesce(T... items) {
    if (items == null) {
      return null;
    }
    for (T i : items) {
      if (i != null) {
        return i;
      }
    }
    return null;
  }
}
