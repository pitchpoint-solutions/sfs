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
import io.vertx.core.streams.ReadStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.InflaterOutputStream;

import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class InflaterReadStream implements EndableReadStream<Buffer> {

    private static final Logger LOGGER = getLogger(InflaterReadStream.class);
    private final EndableReadStream<Buffer> delegate;
    private Handler<Void> delegateEndHandler;
    private Handler<Throwable> delegateExceptionHandler;
    private Handler<Buffer> delegateDataHandler;
    private boolean ended = false;
    private Handler<Buffer> dataHandler;
    private InflaterOutputStream inflaterOutputStream;
    private HandlerOutputStream handlerOutputStream;
    private Handler<Void> endHandler;


    public InflaterReadStream(EndableReadStream<Buffer> delegate) {
        this.delegate = delegate;

        this.endHandler = event -> {
            ended = true;
            try {
                inflaterOutputStream.close();
            } catch (IOException e) {
                handleError(e);
            }
        };

        this.handlerOutputStream = new HandlerOutputStream(
                event -> {
                    if (delegateDataHandler != null) {
                        delegateDataHandler.handle(event);
                    }
                    if (ended && delegateEndHandler != null) {
                        Handler<Void> handler = delegateEndHandler;
                        delegateEndHandler = null;
                        handler.handle(null);
                    }
                });

        this.inflaterOutputStream = new InflaterOutputStream(handlerOutputStream);

        this.dataHandler = event -> {
            try {
                inflaterOutputStream.write(event.getBytes());
            } catch (IOException e) {
                handleError(e);
            }
        };
    }

    @Override
    public boolean isEnded() {
        return delegate.isEnded();
    }

    @Override
    public InflaterReadStream endHandler(Handler<Void> endHandler) {
        this.delegateEndHandler = endHandler;
        if (endHandler != null) {
            this.delegate.endHandler(this.endHandler);
        } else {
            this.delegate.endHandler(null);
        }
        return this;
    }

    @Override
    public InflaterReadStream handler(Handler<Buffer> handler) {
        delegateDataHandler = handler;
        if (handler == null) {
            delegate.handler(null);
        } else {
            delegate.handler(dataHandler);
        }
        return this;
    }

    @Override
    public InflaterReadStream pause() {
        delegate.pause();
        return this;
    }

    @Override
    public InflaterReadStream resume() {
        delegate.resume();
        return this;
    }

    @Override
    public ReadStream<Buffer> fetch(long amount) {
        return this;
    }

    @Override
    public InflaterReadStream exceptionHandler(Handler<Throwable> handler) {
        this.delegateExceptionHandler = handler;
        delegate.exceptionHandler(handler);
        return this;
    }


    protected void handleError(Throwable e) {
        if (delegateExceptionHandler != null) {
            delegateExceptionHandler.handle(e);
        } else {
            LOGGER.error("Unhandled Exception", e);
        }
    }

    protected static class HandlerOutputStream extends OutputStream {

        private final Handler<Buffer> handler;

        public HandlerOutputStream(Handler<Buffer> handler) {
            this.handler = handler;
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void write(byte[] b) throws IOException {
            handler.handle(buffer(b));
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            handler.handle(buffer(len)
                    .appendBytes(b, off, len));
        }

        @Override
        public void write(int b) throws IOException {
            handler.handle(buffer(1)
                    .appendByte((byte) b));
        }
    }
}
