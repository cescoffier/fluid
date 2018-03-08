package me.escoffier.fluid.inject;

import io.reactivex.Flowable;
import me.escoffier.fluid.annotations.Inbound;
import me.escoffier.fluid.annotations.Transformation;
import me.escoffier.fluid.models.Data;
import me.escoffier.fluid.models.Sink;
import me.escoffier.fluid.models.Source;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class MediatorRequiringPublisher {

  @Inbound("my-source")
  private Publisher<Data<String>> source;

  public static List<String> SPY = new ArrayList<>();

  @Transformation
  public void transform() {
    Flowable.fromPublisher(source).map(Data::payload).map(String::toUpperCase).subscribe(s -> SPY.add(s));
  }
}
