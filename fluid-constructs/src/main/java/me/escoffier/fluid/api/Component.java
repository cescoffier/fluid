package me.escoffier.fluid.api;

import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface Component {

  List<InPort> inputs();

  List<OutPort> outputs();

  void onCreation();

  void onAssembly();

  Component connectTo(Component next);

  Component run();
}
