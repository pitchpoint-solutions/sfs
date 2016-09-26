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
import io.vertx.core.json.JsonObject;
import org.sfs.util.HttpRequestValidationException;

import java.net.HttpURLConnection;

public class LimitedWriteEndableWriteStream implements BufferEndableWriteStream {

    private final BufferEndableWriteStream delegate;
    private final long length;
    private long bytesWritten = 0;

    public LimitedWriteEndableWriteStream(BufferEndableWriteStream delegate, long length) {
        this.delegate = delegate;
        this.length = length;
    }

    @Override
    public LimitedWriteEndableWriteStream write(Buffer data) {
        bytesWritten += data.length();
        if (bytesWritten > length) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", String.format("Content-Length must be between 0 and %d", length));

            throw new HttpRequestValidationException(HttpURLConnection.HTTP_BAD_REQUEST, jsonObject);
        }
        delegate.write(data);
        return this;
    }

    @Override
    public LimitedWriteEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public LimitedWriteEndableWriteStream drainHandler(Handler<Void> handler) {
        delegate.drainHandler(handler);
        return this;
    }

    @Override
    public LimitedWriteEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public LimitedWriteEndableWriteStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        delegate.end(buffer);
    }

    @Override
    public void end() {
        delegate.end();
    }
}
