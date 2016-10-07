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

import static com.google.common.math.LongMath.checkedAdd;

public class CountingEndableWriteStream implements BufferEndableWriteStream {

    private final BufferEndableWriteStream delegate;
    private long count = 0;

    public CountingEndableWriteStream(BufferEndableWriteStream delegate) {
        this.delegate = delegate;
    }

    public long count() {
        return count;
    }

    @Override
    public CountingEndableWriteStream write(Buffer data) {
        count = checkedAdd(count, data.length());
        delegate.write(data);
        return this;
    }

    @Override
    public CountingEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public CountingEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public CountingEndableWriteStream drainHandler(Handler<Void> handler) {
        delegate.drainHandler(handler);
        return this;
    }

    @Override
    public CountingEndableWriteStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        count = checkedAdd(count, buffer.length());
        delegate.end(buffer);
    }

    @Override
    public void end() {
        delegate.end();
    }
}
