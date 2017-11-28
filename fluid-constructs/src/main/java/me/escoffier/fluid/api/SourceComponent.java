package me.escoffier.fluid.api;

import java.util.Collections;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface SourceComponent<T> extends Component {

  @Override
  default List<InPort> inputs() {
    return Collections.emptyList();
  }

  @Override
  default List<OutPort> outputs() {
    return Collections.singletonList(output());
  }

  OutPort<T> output();

}
