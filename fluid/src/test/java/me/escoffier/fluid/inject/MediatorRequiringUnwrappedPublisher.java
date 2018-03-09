package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Transformation;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MediatorRequiringUnwrappedPublisher {

  @Inbound("my-source")
  private Publisher<String> source;

  public static List<String> SPY = new ArrayList<>();

  @Transformation
  public void transform() {
    Flowable.fromPublisher(source).map(String::toUpperCase).subscribe(s -> SPY.add(s));
  }
}
