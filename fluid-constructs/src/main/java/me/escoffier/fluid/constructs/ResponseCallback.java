package me.escoffier.fluid.constructs;

import io.reactivex.Completable;

public interface ResponseCallback {

    Completable reply(Object response);

}
