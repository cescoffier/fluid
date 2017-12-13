package me.escoffier.fluid.constructs;

import java.util.Optional;

public final class CommonHeaders {

  public static final String ORIGINAL = "fluid.original";

  public static final String ADDRESS = "fluid.address";

  public static final String KEY = "fluid.key";


  private CommonHeaders() {
    // Avoid direct instantiation.
  }


  @SuppressWarnings("unchecked")
  public static <T> T original(Data data) {
    return (T) data.get(ORIGINAL);
  }

  @SuppressWarnings("unchecked")
  public static <T> Optional<T> originalOpt(Data data) {
    return data.getOpt(ORIGINAL);
  }

  public static String address(Data data) {
    return (String) data.get(ADDRESS);
  }

  @SuppressWarnings("unchecked")
  public static Optional<String> addressOpt(Data data) {
    return data.getOpt(ADDRESS);
  }

  public static String key(Data data) {
    return (String) data.get(KEY);
  }

  @SuppressWarnings("unchecked")
  public static Optional<String> keyOpt(Data data) {
    return data.getOpt(KEY);
  }

}
