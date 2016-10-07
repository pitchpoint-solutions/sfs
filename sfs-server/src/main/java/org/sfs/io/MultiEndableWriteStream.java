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
import io.vertx.core.logging.Logger;
import org.sfs.rx.ResultMemoizeHandler;
import rx.Subscriber;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static rx.Observable.create;
import static rx.Observable.from;

public class MultiEndableWriteStream implements BufferEndableWriteStream {

    private static final Logger LOGGER = getLogger(MultiEndableWriteStream.class);
    protected final BufferEndableWriteStream[] delegateWriteStreams;
    private Handler<Void> delegateEndHandler;
    private Handler<Void> delegateDrainHandler;
    private Handler<Throwable> delegateErrorHandler;
    private boolean ended = false;
    private Buffer endBuffer = null;


    public MultiEndableWriteStream(BufferEndableWriteStream[] writeStreams) {
        this.delegateWriteStreams = writeStreams;
    }


    public MultiEndableWriteStream(BufferEndableWriteStream writeStream, BufferEndableWriteStream... writeStreams) {
        this.delegateWriteStreams = new BufferEndableWriteStream[1 + writeStreams.length];
        this.delegateWriteStreams[0] = writeStream;
        for (int i = 0; i < writeStreams.length; i++) {
            this.delegateWriteStreams[i + 1] = writeStreams[i];
        }
    }

    @Override
    public MultiEndableWriteStream write(Buffer data) {
        checkState(!ended, "Already ended");
        for (BufferEndableWriteStream writeStream : delegateWriteStreams) {
            writeStream.write(data);
        }
        return this;
    }

    @Override
    public MultiEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        for (BufferEndableWriteStream writeStream : delegateWriteStreams) {
            writeStream.setWriteQueueMaxSize(maxSize);
        }
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        for (BufferEndableWriteStream writeStream : delegateWriteStreams) {
            if (writeStream.writeQueueFull()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public MultiEndableWriteStream drainHandler(Handler<Void> handler) {
        this.delegateDrainHandler = handler;
        handleDrain();
        return this;
    }

    @Override
    public MultiEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        this.delegateErrorHandler = handler;
        for (BufferEndableWriteStream writeStream : delegateWriteStreams) {
            writeStream.exceptionHandler(this::handleError);
        }
        return this;
    }

    @Override
    public MultiEndableWriteStream endHandler(Handler<Void> handler) {
        this.delegateEndHandler = handler;
        handleEnd();
        return this;
    }

    @Override
    public void end(final Buffer buffer) {
        checkState(!ended, "Already ended");
        ended = true;
        endBuffer = buffer;
        handleEnd();
    }

    @Override
    public void end() {
        checkState(!ended, "Already ended");
        ended = true;
        handleEnd();
    }

    protected void handleDrain() {
        if (delegateDrainHandler != null) {
            Handler<Void> handler = delegateDrainHandler;
            delegateDrainHandler = null;
            from(delegateWriteStreams)
                    .flatMap(endableWriteStream -> {
                        ResultMemoizeHandler<Void> h = new ResultMemoizeHandler<>();
                        endableWriteStream.drainHandler(h);
                        return create(h.subscribe);
                    })
                    .subscribe(new Subscriber<Void>() {
                        @Override
                        public void onCompleted() {
                            handler.handle(null);
                        }

                        @Override
                        public void onError(Throwable e) {
                            handleError(e);
                        }

                        @Override
                        public void onNext(Void aVoid) {
                        }
                    });
        }
    }

    protected void handleEnd() {
        Handler<Void> handler = delegateEndHandler;
        if (ended && handler != null) {
            delegateEndHandler = null;
            from(delegateWriteStreams)
                    .flatMap(endableWriteStream -> {
                        ResultMemoizeHandler<Void> h = new ResultMemoizeHandler<>();
                        endableWriteStream.endHandler(h);
                        if (endBuffer != null) {
                            Buffer data = endBuffer;
                            endBuffer = null;
                            endableWriteStream.end(data);
                        } else {
                            endableWriteStream.end();
                        }
                        return create(h.subscribe);
                    })
                    .subscribe(new Subscriber<Void>() {
                        @Override
                        public void onCompleted() {
                            handler.handle(null);
                        }

                        @Override
                        public void onError(Throwable e) {
                            handleError(e);
                        }

                        @Override
                        public void onNext(Void aVoid) {
                        }
                    });
        }
    }

    protected void handleError(Throwable e) {
        if (delegateErrorHandler != null) {
            Handler<Throwable> handler = delegateErrorHandler;
            delegateErrorHandler = null;
            handler.handle(e);
        } else {
            LOGGER.error("Unhandled Exception", e);
        }
    }
}
