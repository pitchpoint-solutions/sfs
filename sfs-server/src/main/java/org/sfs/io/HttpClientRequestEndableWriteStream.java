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
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.logging.Logger;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class HttpClientRequestEndableWriteStream implements BufferEndableWriteStream {

    private static final Logger LOGGER = getLogger(HttpClientRequestEndableWriteStream.class);
    private final HttpClientRequest httpClientRequest;
    private Handler<Void> delegateEndHandler;
    private boolean ended = false;

    public HttpClientRequestEndableWriteStream(HttpClientRequest httpClientRequest) {
        this.httpClientRequest = httpClientRequest;
    }

    @Override
    public HttpClientRequestEndableWriteStream write(Buffer data) {
        checkNotEnded();
        httpClientRequest.write(data);
        return this;
    }

    public HttpClientRequestEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        checkNotEnded();
        httpClientRequest.exceptionHandler(handler);
        return this;
    }

    @Override
    public HttpClientRequestEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        checkNotEnded();
        httpClientRequest.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return httpClientRequest.writeQueueFull();
    }

    @Override
    public HttpClientRequestEndableWriteStream drainHandler(Handler<Void> handler) {
        checkNotEnded();
        httpClientRequest.drainHandler(handler);
        return this;
    }

    public HttpClientRequestEndableWriteStream endHandler(Handler<Void> endHandler) {
        checkNotEnded();
        this.delegateEndHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        checkNotEnded();
        ended = true;
        httpClientRequest.end(buffer);
        handleEnd();
    }

    @Override
    public void end() {
        checkNotEnded();
        ended = true;
        httpClientRequest.end();
        handleEnd();
    }

    protected void handleEnd() {
        if (ended) {
            Handler<Void> handler = delegateEndHandler;
            if (handler != null) {
                delegateEndHandler = null;
                handler.handle(null);
            }
        }
    }

    protected void checkNotEnded() {
        checkState(!ended, "WriteStream ended");
    }
}
