/*
 * Copyright 2016 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.io;


import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.logging.Logger;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Subscriber;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class AsyncFileEndableWriteStream implements BufferEndableWriteStream {

    private static final Logger LOGGER = getLogger(AsyncFileEndableWriteStream.class);
    private final AsyncFile delegate;
    private Handler<Void> endHandler;
    private boolean ended = false;
    private Handler<Throwable> delegateExceptionHandler;

    public AsyncFileEndableWriteStream(AsyncFile delegate) {
        this.delegate = delegate;
    }

    public AsyncFile getDelegate() {
        return delegate;
    }

    @Override
    public AsyncFileEndableWriteStream write(Buffer data) {
        checkNotEnded();
        delegate.write(data);
        return this;
    }

    @Override
    public AsyncFileEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        checkNotEnded();
        delegateExceptionHandler = handler;
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public AsyncFileEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        checkNotEnded();
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public AsyncFileEndableWriteStream drainHandler(Handler<Void> handler) {
        checkNotEnded();
        delegate.drainHandler(handler);
        return this;
    }

    @Override
    public AsyncFileEndableWriteStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        doEnded();
        return this;
    }

    @Override
    public final void end(Buffer buffer) {
        checkNotEnded();
        ended = true;
        delegate.write(buffer);
        endInternal();
    }

    @Override
    public final void end() {
        checkNotEnded();
        ended = true;
        endInternal();
    }

    private void endInternal() {
        ObservableFuture<Void> handler = RxHelper.observableFuture();
        delegate.close(handler.toHandler());
        handler
                .subscribe(new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        doEnded();
                    }

                    @Override
                    public void onError(Throwable e) {
                        if (delegateExceptionHandler != null) {
                            delegateExceptionHandler.handle(e);
                        } else {
                            LOGGER.error("Unhandled Exception", e);
                        }
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }

    private void doEnded() {
        if (ended) {
            if (endHandler != null) {
                Handler<Void> handler = endHandler;
                endHandler = null;
                handler.handle(null);
            }
        }
    }

    protected void checkNotEnded() {
        checkState(!ended, "WriteStream ended");
    }
}
