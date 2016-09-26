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

public class BufferedEndableWriteStream implements BufferEndableWriteStream {

    private BufferEndableWriteStream delegate;
    private final int size;
    private Buffer buffer;

    public BufferedEndableWriteStream(BufferEndableWriteStream delegate) {
        this(delegate, 8192);
    }

    public BufferedEndableWriteStream(BufferEndableWriteStream delegate, int size) {
        this.delegate = delegate;
        this.size = size;
        this.buffer = Buffer.buffer(size);
    }

    @Override
    public BufferedEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public BufferedEndableWriteStream write(Buffer data) {
        if (buffer.length() + data.length() >= size) {
            Buffer b = buffer;
            buffer = Buffer.buffer(size);
            delegate.write(b);
            delegate.write(data);
        } else {
            buffer.appendBuffer(data);
        }
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public BufferedEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public BufferedEndableWriteStream drainHandler(Handler<Void> handler) {
        delegate.drainHandler(handler);
        return this;
    }

    @Override
    public BufferedEndableWriteStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public void end(Buffer data) {
        if (buffer.length() > 0) {
            buffer.appendBuffer(data);
            Buffer b = buffer;
            buffer = null;
            delegate.end(b);
        } else {
            delegate.end(data);
        }
    }

    @Override
    public void end() {
        if (buffer.length() > 0) {
            Buffer b = buffer;
            buffer = null;
            delegate.end(b);
        } else {
            delegate.end();
        }
    }
}
