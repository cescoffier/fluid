package me.escoffier.fluid.api.components;

import com.sun.tools.javac.comp.Flow;
import io.reactivex.Flowable;
import me.escoffier.fluid.api.OutPort;

import java.lang.reflect.ParameterizedType;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Output<T> implements OutPort<T> {

  private final String name;
  private final Class<T> clazz;

  private Flowable<T> flowable;

  public Output(String name, Class<T> clazz) {
    this.name = name;
    this.clazz = clazz;
  }

  @SuppressWarnings("unchecked")
  public Output(String name) {
    this.name = name;
    this.clazz = (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }


  @Override
  public String name() {
    return name;
  }

  @Override
  public Class<T> type() {
    return clazz;
  }

  @Override
  public synchronized void connect(Flowable<T> flow) {
    this.flowable = flow;
  }

  @Override
  public synchronized Flowable<T> flow() {
    return flowable;
  }
}
