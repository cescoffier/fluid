package me.escoffier.fluid.constructs;

import java.util.Optional;

public final class CommonHeaders {

  public static final String KEY = "fluid.key";

  public static final String ORIGINAL = "fluid.original";

  private CommonHeaders() {
  }

  public static String key(Data data) {
    return (String) data.get(KEY);
  }

  public static Optional<String> keyOpt(Data data) {
    return data.getOpt(KEY);
  }

  public static <T> T original(Data data) {
    return (T) data.get(ORIGINAL);
  }

  public static <T> Optional<T> originalOpt(Data data) {
    return data.getOpt(ORIGINAL);
  }

}
