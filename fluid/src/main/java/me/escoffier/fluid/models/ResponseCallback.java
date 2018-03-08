package me.escoffier.fluid.models;

import io.reactivex.Completable;

public interface ResponseCallback {

    Completable reply(Object response);

}
