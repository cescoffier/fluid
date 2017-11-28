package me.escoffier.fluid.api;

import java.util.Collections;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface SinkComponent<T> extends Component {

  @Override
  default List<InPort> inputs() {
    return Collections.singletonList(input());
  }

  @Override
  default List<OutPort> outputs() {
    return Collections.emptyList();
  }

  InPort<T> input();

}
