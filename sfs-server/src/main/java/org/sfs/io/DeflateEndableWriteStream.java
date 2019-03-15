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

import java.io.IOException;
import java.util.zip.DeflaterOutputStream;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class DeflateEndableWriteStream implements BufferEndableWriteStream {

    private static final Logger LOGGER = getLogger(DeflateEndableWriteStream.class);
    private final BufferEndableWriteStream delegate;
    private Handler<Throwable> delegateExceptionHandler;
    private DeflaterOutputStream deflatorOutputStream;
    private BufferEndableWriteStreamOutputStream bufferEndableWriteStreamOutputStream;
    private boolean ended = false;

    public DeflateEndableWriteStream(BufferEndableWriteStream delegate) {
        this.delegate = delegate;
        this.bufferEndableWriteStreamOutputStream = new BufferEndableWriteStreamOutputStream(delegate);
        this.deflatorOutputStream = new DeflaterOutputStream(bufferEndableWriteStreamOutputStream);
    }

    @Override
    public boolean isEnded() {
        return delegate != null && delegate.isEnded();
    }

    @Override
    public DeflateEndableWriteStream drainHandler(Handler<Void> handler) {
        checkNotEnded();
        delegate.drainHandler(handler);
        return this;
    }

    @Override
    public DeflateEndableWriteStream write(Buffer data) {
        checkNotEnded();
        try {
            deflatorOutputStream.write(data.getBytes());
        } catch (IOException e) {
            handleError(e);
        }
        return this;
    }

    protected void checkNotEnded() {
        checkState(!ended, "Already ended");
    }

    @Override
    public DeflateEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        delegateExceptionHandler = handler;
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public DeflateEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public DeflateEndableWriteStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public void end(Buffer data) {
        checkNotEnded();
        ended = true;
        try {
            deflatorOutputStream.write(data.getBytes());
            deflatorOutputStream.close();
        } catch (IOException e) {
            handleError(e);
        }
    }

    @Override
    public void end() {
        checkNotEnded();
        ended = true;
        try {
            deflatorOutputStream.close();
        } catch (IOException e) {
            handleError(e);
        }
    }

    protected void handleError(Throwable e) {
        if (delegateExceptionHandler != null) {
            delegateExceptionHandler.handle(e);
        } else {
            LOGGER.error("Unhandled Exception", e);
        }
    }
}
