package me.escoffier.fluid.api;

import java.util.Collections;
import java.util.List;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface FanOutComponent<IN> extends Component {

  InPort<IN> input();

  @Override
  default List<InPort> inputs() {
    return Collections.singletonList(input());
  }

}
