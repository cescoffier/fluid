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
  public static <T> T original(Message message) {
    return (T) message.get(ORIGINAL);
  }

  @SuppressWarnings("unchecked")
  public static <T> Optional<T> originalOpt(Message message) {
    return message.getOpt(ORIGINAL);
  }

  public static String address(Message message) {
    return (String) message.get(ADDRESS);
  }

  @SuppressWarnings("unchecked")
  public static Optional<String> addressOpt(Message message) {
    return message.getOpt(ADDRESS);
  }

  public static String key(Message message) {
    return (String) message.get(KEY);
  }

  @SuppressWarnings("unchecked")
  public static Optional<String> keyOpt(Message message) {
    return message.getOpt(KEY);
  }

  public static ResponseCallback responseCallback(Message message) {
    return (ResponseCallback) message.get(RESPONSE_CALLBACK);
  }

  public static Optional<ResponseCallback> responseCallbackOpt(Message message) {
    return (Optional<ResponseCallback>) message.getOpt(RESPONSE_CALLBACK);
  }

}
