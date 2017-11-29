package me.escoffier.fluid.constructs;

import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.file.OpenOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.buffer.Buffer;
import io.vertx.reactivex.core.file.AsyncFile;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class FileSink implements Sink<String> {

    private final Vertx vertx;
    private final Single<AsyncFile> flow;

    public FileSink(Vertx vertx, String path) {
        this.vertx = vertx;
        flow = this.vertx.fileSystem()
            .rxOpen(path, new OpenOptions().setWrite(true))
            .cache();

    }

    @Override
    public Completable dispatch(Data<String> data) {
        return flow
            .doOnSuccess(file -> file.write(Buffer.buffer(data.item()))).toCompletable();
    }
}
