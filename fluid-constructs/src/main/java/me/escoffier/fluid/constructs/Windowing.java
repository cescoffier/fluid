package me.escoffier.fluid.constructs;

import io.reactivex.Flowable;

/**
 * A set of common {@link WindowOperator}s.
 */
public class Windowing {

  /**
   * Creates a window based on the number of items. The window is <em>closed</em>, meaning that when the processing
   * starts, all the items have been received.
   *
   * @param size the size, must be strictly positive.
   * @param <T>  the type of payload
   * @return the operator
   */
  public static <T> WindowOperator<T> bySize(int size) {
    return source -> source.buffer(size)
      .map(Window::new);
  }

  /**
   * Creates a window based on the number of items. Depending on the {@code open} parameter, the window is <em>closed</em>,
   * meaning that when the processing starts, all the items have been received; or <em>open</em> meaning that the
   * processing starts as soon as the first item is received, but without any guarantee when other items will be available.
   *
   * @param size the size, must be strictly positive.
   * @param open whether or not the window is open
   * @param <T>  the type of payload
   * @return the operator
   */
  public static <T> WindowOperator<T> bySize(int size, boolean open) {
    if (!open) {
      return bySize(size);
    } else {
      return source -> source.window(size)
        .map(Window::new);
    }
  }

  /**
   * Creates a window containing all the items from the source. The window is <em>closed</em>, meaning that when the
   * processing starts, all the items have been received. Do not use this operator on infinite stream.
   *
   * @param <T> the type of payload
   * @return the operator
   */
  public static <T> WindowOperator<T> all() {
    return source -> source.toList().map(Window::new).toFlowable();
  }

  /**
   * Creates a window containing all the items from the source. The window is <em>closed</em>, meaning that when the
   * processing starts, all the items have been received. Do not use this operator on infinite stream.
   * <p>
   * Depending on the {@code open} parameter, the window is <em>closed</em>,
   * meaning that when the processing starts, all the items have been received; or <em>open</em> meaning that the
   * processing starts as soon as the first item is received, but without any guarantee when other items will be available.
   *
   * @param open whether or not the window is open
   * @param <T>  the type of payload
   * @return the operator
   */
  public static <T> WindowOperator<T> all(boolean open) {
    if (!open) {
      return all();
    } else {
      return source -> Flowable.just(new Window<T>(source));
    }
  }
}
