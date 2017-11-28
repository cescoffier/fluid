package me.escoffier.fluid.api.components;

import me.escoffier.fluid.api.Component;
import me.escoffier.fluid.api.FanInComponent;
import me.escoffier.fluid.api.InPort;
import me.escoffier.fluid.api.OutPort;

import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Merge<T> implements FanInComponent<T> {
  @Override
  public OutPort<T> output() {
    return null;
  }

  @Override
  public List<InPort> inputs() {
    return null;
  }

  @Override
  public void onCreation() {

  }

  @Override
  public void onAssembly() {

  }

  @Override
  public Component connectTo(Component next) {
    return null;
  }
}
