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

package org.sfs.util;


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import org.sfs.Server;
import org.sfs.VertxContext;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.buffer.Buffer.buffer;

public class KeepAliveHttpServerResponse implements HttpServerResponse {

    private final VertxContext<Server> vertxContext;
    public static final String DELIMITER = "\n";
    public static final Buffer DELIMITER_BUFFER = buffer(DELIMITER, UTF_8.toString());
    private final HttpServerResponse delegate;
    private final long timeout;
    private Long periodic;
    private boolean keepAliveRunning = false;
    private boolean keepAliveStarted = false;
    private Handler<Throwable> delegateExceptionHandler;

    public KeepAliveHttpServerResponse(VertxContext<Server> vertxContext, long timeout, TimeUnit timeUnit, HttpServerResponse delegate) {
        this.vertxContext = vertxContext;
        this.delegate = delegate;
        this.timeout = timeUnit.toMillis(timeout);
        delegate.setChunked(true);
        delegate.exceptionHandler(exception -> stopKeepAlive(event1 -> {
            if (delegateExceptionHandler != null) {
                delegateExceptionHandler.handle(exception);
            }
        }));
        startKeepAlive();
    }

    public KeepAliveHttpServerResponse startKeepAlive() {
        checkState(!keepAliveStarted, "Keep Alive is already running");
        keepAliveStarted = true;
        delegate.write(DELIMITER_BUFFER);
        periodic = vertxContext.vertx().setPeriodic(timeout, event -> {
            keepAliveRunning = true;
            try {
                if (keepAliveStarted) {
                    delegate.write(DELIMITER_BUFFER);
                }
            } finally {
                keepAliveRunning = false;
            }
        });
        return this;
    }

    public KeepAliveHttpServerResponse stopKeepAlive(Handler<Void> handler) {
        keepAliveStarted = false;
        Long p = periodic;
        periodic = null;
        if (p != null) {
            vertxContext.vertx().cancelTimer(p);
        }
        if (keepAliveRunning) {
            vertxContext.vertx().runOnContext(event -> stopKeepAlive(handler));
        } else {
            handler.handle(null);
        }
        return this;
    }

    @Override
    public int getStatusCode() {
        return delegate.getStatusCode();
    }

    @Override
    public HttpServerResponse setStatusCode(int statusCode) {
        return delegate.setStatusCode(statusCode);
    }

    @Override
    public String getStatusMessage() {
        return delegate.getStatusMessage();
    }

    @Override
    public HttpServerResponse setStatusMessage(String statusMessage) {
        delegate.setStatusMessage(statusMessage);
        return this;
    }

    @Override
    public HttpServerResponse setChunked(boolean chunked) {
        delegate.setChunked(chunked);
        return this;
    }

    @Override
    public boolean isChunked() {
        return delegate.isChunked();
    }

    @Override
    public MultiMap headers() {
        return delegate.headers();
    }

    @Override
    public HttpServerResponse putHeader(String name, String value) {
        delegate.putHeader(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putHeader(CharSequence name, CharSequence value) {
        delegate.putHeader(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putHeader(String name, Iterable<String> values) {
        delegate.putHeader(name, values);
        return this;
    }

    @Override
    public HttpServerResponse putHeader(CharSequence name, Iterable<CharSequence> values) {
        delegate.putHeader(name, values);
        return this;
    }

    @Override
    public MultiMap trailers() {
        return delegate.trailers();
    }

    @Override
    public HttpServerResponse putTrailer(String name, String value) {
        delegate.putTrailer(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putTrailer(CharSequence name, CharSequence value) {
        delegate.putTrailer(name, value);
        return this;
    }

    @Override
    public HttpServerResponse putTrailer(String name, Iterable<String> values) {
        delegate.putTrailer(name, values);
        return this;
    }

    @Override
    public HttpServerResponse putTrailer(CharSequence name, Iterable<CharSequence> value) {
        delegate.putTrailer(name, value);
        return this;
    }

    @Override
    public HttpServerResponse closeHandler(Handler<Void> handler) {
        delegate.closeHandler(handler);
        return this;
    }

    @Override
    public HttpServerResponse write(Buffer chunk) {
        stopKeepAlive(event -> delegate.write(chunk));
        return this;
    }

    @Override
    public HttpServerResponse write(String chunk, String enc) {
        stopKeepAlive(event -> delegate.write(chunk, enc));
        return this;
    }

    @Override
    public HttpServerResponse write(String chunk) {
        stopKeepAlive(event -> delegate.write(chunk));
        return this;
    }

    @Override
    public void end(String chunk) {
        stopKeepAlive(event -> delegate.end(chunk));
    }

    @Override
    public void end(String chunk, String enc) {
        stopKeepAlive(event -> delegate.end(chunk, enc));
    }

    @Override
    public void end(Buffer chunk) {
        stopKeepAlive(event -> delegate.end(chunk));
    }

    @Override
    public void end() {
        stopKeepAlive(event -> delegate.end());
    }

    @Override
    public HttpServerResponse writeContinue() {
        stopKeepAlive(event -> delegate.writeContinue());
        return this;
    }

    @Override
    public HttpServerResponse sendFile(String filename, long offset, long length) {
        stopKeepAlive(event -> delegate.sendFile(filename, offset, length));
        return this;
    }

    @Override
    public HttpServerResponse sendFile(String filename, long offset, long length, Handler<AsyncResult<Void>> resultHandler) {
        stopKeepAlive(event -> delegate.sendFile(filename, offset, length, resultHandler));
        return this;
    }

    @Override
    public boolean ended() {
        return delegate.ended();
    }

    @Override
    public boolean closed() {
        return delegate.closed();
    }

    @Override
    public boolean headWritten() {
        return delegate.headWritten();
    }

    @Override
    public HttpServerResponse headersEndHandler(Handler<Void> handler) {
        delegate.headersEndHandler(handler);
        return this;
    }

    @Override
    public HttpServerResponse bodyEndHandler(Handler<Void> handler) {
        delegate.bodyEndHandler(handler);
        return this;
    }

    @Override
    public long bytesWritten() {
        return delegate.bytesWritten();
    }

    @Override
    public int streamId() {
        return delegate.streamId();
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String host, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
        stopKeepAlive(event -> delegate.push(method, host, path, handler));
        return this;
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
        stopKeepAlive(event -> delegate.push(method, path, headers, handler));
        return this;
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String path, Handler<AsyncResult<HttpServerResponse>> handler) {
        stopKeepAlive(event -> delegate.push(method, path, handler));
        return this;
    }

    @Override
    public HttpServerResponse push(HttpMethod method, String host, String path, MultiMap headers, Handler<AsyncResult<HttpServerResponse>> handler) {
        stopKeepAlive(event -> delegate.push(method, host, path, headers, handler));
        return this;
    }

    @Override
    public void reset(long code) {
        stopKeepAlive(event -> delegate.reset(code));
    }

    @Override
    public HttpServerResponse writeCustomFrame(int type, int flags, Buffer payload) {
        stopKeepAlive(event -> delegate.writeCustomFrame(type, flags, payload));
        return this;
    }

    @Override
    public void close() {
        stopKeepAlive(event -> delegate.close());
    }

    @Override
    public HttpServerResponse exceptionHandler(Handler<Throwable> handler) {
        delegateExceptionHandler = handler;
        return this;
    }

    @Override
    public HttpServerResponse setWriteQueueMaxSize(int maxSize) {
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public HttpServerResponse drainHandler(Handler<Void> handler) {
        delegate.drainHandler(handler);
        return this;
    }
}
