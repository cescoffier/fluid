package me.escoffier.fluid.constructs;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Windowing {

  public static <T> WindowOperator<T> bySize(int size) {
    return source -> source.buffer(size)
        .map(Window::new);
  }
}