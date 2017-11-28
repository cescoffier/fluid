package me.escoffier.fluid.api.components;

import me.escoffier.fluid.api.Component;
import me.escoffier.fluid.api.InPort;
import me.escoffier.fluid.api.OutPort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class AbstractComponent implements Component {

  private final List<OutPort> outputs;
  private List<InPort> inputs;

  public AbstractComponent(List<InPort> in, List<OutPort> out) {
    this.inputs = Collections.unmodifiableList(new ArrayList<>(in));
    this.outputs = Collections.unmodifiableList(new ArrayList<>(out));
  }

  @Override
  public List<InPort> inputs() {
    return inputs;
  }

  @Override
  public List<OutPort> outputs() {
    return outputs;
  }

  @Override
  public void onCreation() {
    // Do nothing by default
  }

  @Override
  public void onAssembly() {
     // Do nothing by default
  }

  @Override
  public Component connectTo(Component next) {
    // Do nothing by default
  }

  @Override
  public Component run() {
    return null;
  }
}
