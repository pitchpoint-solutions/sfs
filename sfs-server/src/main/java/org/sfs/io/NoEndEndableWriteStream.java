/*
 *
 * Copyright (C) 2009 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.io;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

import static com.google.common.base.Preconditions.checkState;

public class NoEndEndableWriteStream implements BufferEndableWriteStream {

    private final BufferEndableWriteStream delegate;
    private Handler<Void> endHandler;
    private boolean ended = false;

    public NoEndEndableWriteStream(BufferEndableWriteStream delegate) {
        this.delegate = delegate;
    }

    @Override
    public NoEndEndableWriteStream write(Buffer data) {
        checkNotEnded();
        delegate.write(data);
        return this;
    }

    public NoEndEndableWriteStream exceptionHandler(Handler handler) {
        checkNotEnded();
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public NoEndEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        checkNotEnded();
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public NoEndEndableWriteStream drainHandler(Handler handler) {
        checkNotEnded();
        delegate.drainHandler(handler);
        return this;
    }

    public NoEndEndableWriteStream endHandler(Handler endHandler) {
        checkNotEnded();
        this.endHandler = endHandler;
        doEnded();
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        checkNotEnded();
        ended = true;
        delegate.write(buffer);
        doEnded();
    }

    @Override
    public void end() {
        checkNotEnded();
        ended = true;
        doEnded();
    }

    private void doEnded() {
        if (ended && endHandler != null) {
            Handler<Void> handler = endHandler;
            endHandler = null;
            handler.handle(null);
        }
    }

    protected void checkNotEnded() {
        checkState(!ended, "WriteStream ended");
    }
}
