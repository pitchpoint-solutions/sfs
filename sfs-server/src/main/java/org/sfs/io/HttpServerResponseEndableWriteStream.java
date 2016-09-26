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
import io.vertx.core.http.HttpServerResponse;

import static com.google.common.base.Preconditions.checkState;

public class HttpServerResponseEndableWriteStream implements BufferEndableWriteStream {

    private final HttpServerResponse httpServerResponse;
    private Handler<Void> endHandler;
    private boolean ended = false;

    public HttpServerResponseEndableWriteStream(HttpServerResponse httpServerResponse) {
        this.httpServerResponse = httpServerResponse;
    }

    @Override
    public HttpServerResponseEndableWriteStream write(Buffer data) {
        checkNotEnded();
        httpServerResponse.write(data);
        return this;
    }

    public HttpServerResponseEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        checkNotEnded();
        httpServerResponse.exceptionHandler(handler);
        return this;
    }

    @Override
    public HttpServerResponseEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        checkNotEnded();
        httpServerResponse.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return httpServerResponse.writeQueueFull();
    }

    @Override
    public HttpServerResponseEndableWriteStream drainHandler(Handler<Void> handler) {
        checkNotEnded();
        httpServerResponse.drainHandler(handler);
        return this;
    }

    public HttpServerResponseEndableWriteStream endHandler(Handler<Void> endHandler) {
        checkNotEnded();
        this.endHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        checkNotEnded();
        ended = true;
        httpServerResponse.end(buffer);
        handleEnd();
    }

    @Override
    public void end() {
        checkNotEnded();
        ended = true;
        httpServerResponse.end();
        handleEnd();
    }

    protected void checkNotEnded() {
        checkState(!ended, "WriteStream ended");
    }

    protected void handleEnd() {
        if (ended && endHandler != null) {
            Handler<Void> handler = endHandler;
            endHandler = null;
            handler.handle(null);
        }
    }
}
