package me.escoffier.fluid.api;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public interface FlowComponent<IN, OUT> extends FanInComponent<OUT>, FanOutComponent<IN> {

}
