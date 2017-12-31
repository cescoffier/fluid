package me.escoffier.fluid.constructs;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Watermark<T> extends ControlData<T> {

  private final Window<T> window;

  public Watermark(Window<T> window) {
    this.window = Objects.requireNonNull(window);
  }

  public Window window() {
    return window;
  }

  @Override
  public Map<String, Object> headers() {
    return Collections.singletonMap("fluid-window", window);
  }

  @Override
  public String toString() {
    return toString("watermark");
  }

  public static boolean isWatermark(Data d) {
    return d instanceof Watermark;
  }


}
