package me.escoffier.fluid.models;

import java.util.Optional;

public final class CommonHeaders {

  public static final String ORIGINAL = "fluid.original";

  public static final String ADDRESS = "fluid.address";

  public static final String KEY = "fluid.key";

  public static final String RESPONSE_CALLBACK = "fluid.response.callback";

  public static final String GROUP_KEY = "fluid-group-key";


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

  public static ResponseCallback responseCallback(Data data) {
    return (ResponseCallback) data.get(RESPONSE_CALLBACK);
  }

  public static Optional<ResponseCallback> responseCallbackOpt(Data data) {
    return (Optional<ResponseCallback>) data.getOpt(RESPONSE_CALLBACK);
  }

}
