package me.escoffier.fluid.inject;

import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MediatorRequiringSource {

  @Inbound("my-source")
  private Source<String> source;

  public static List<String> SPY = new ArrayList<>();

  @Transformation
  public void transform() {
    source.mapItem(String::toUpperCase).to(Sink.forEachPayload(s -> SPY.add(s)));
  }
}
