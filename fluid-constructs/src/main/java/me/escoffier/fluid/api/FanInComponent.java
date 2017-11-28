package me.escoffier.fluid.api;

import java.util.Collections;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface FanInComponent<OUT> extends Component {

  OutPort<OUT> output();

  @Override
  default List<OutPort> outputs() {
    return Collections.singletonList(output());
  }

}
