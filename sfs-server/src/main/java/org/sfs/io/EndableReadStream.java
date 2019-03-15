/*
 * Copyright 2019 The Simple File Server Authors
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
import io.vertx.core.file.AsyncFile;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;


public interface EndableReadStream<T> extends ReadStream<T> {

    static EndableReadStream<Buffer> from(AsyncFile asyncFile) {

        AtomicBoolean ended = new AtomicBoolean(false);

        AtomicReference<Handler<Void>> handlerRef = new AtomicReference<>(null);

        asyncFile.endHandler(event -> {
            ended.set(true);
            Handler<Void> h = handlerRef.get();
            if (h != null) {
                handlerRef.set(null);
                h.handle(null);
            }
        });

        return new EndableReadStream<Buffer>() {
            @Override
            public boolean isEnded() {
                return ended.get();
            }

            @Override
            public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
                asyncFile.exceptionHandler(handler);
                return this;
            }

            @Override
            public ReadStream<Buffer> handler(Handler<Buffer> handler) {
                asyncFile.handler(handler);
                return this;
            }

            @Override
            public ReadStream<Buffer> pause() {
                asyncFile.pause();
                return this;
            }

            @Override
            public ReadStream<Buffer> resume() {
                asyncFile.resume();
                return this;
            }

            @Override
            public ReadStream<Buffer> fetch(long amount) {
                asyncFile.fetch(amount);
                return this;
            }

            @Override
            public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
                if (ended.get()) {
                    if (endHandler != null) {
                        endHandler.handle(null);
                    }
                } else {
                    handlerRef.set(endHandler);
                }
                return this;
            }
        };
    }

    static EndableReadStream<Buffer> from(HttpClientResponse httpClientResponse) {

        AtomicBoolean ended = new AtomicBoolean(false);

        AtomicReference<Handler<Void>> handlerRef = new AtomicReference<>(null);

        httpClientResponse.endHandler(event -> {
            ended.set(true);
            Handler<Void> h = handlerRef.get();
            if (h != null) {
                handlerRef.set(null);
                h.handle(null);
            }
        });

        return new EndableReadStream<Buffer>() {

            @Override
            public boolean isEnded() {
                return ended.get();
            }

            @Override
            public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
                httpClientResponse.exceptionHandler(handler);
                return this;
            }

            @Override
            public ReadStream<Buffer> handler(Handler<Buffer> handler) {
                httpClientResponse.handler(handler);
                return this;
            }

            @Override
            public ReadStream<Buffer> pause() {
                httpClientResponse.pause();
                return this;
            }

            @Override
            public ReadStream<Buffer> resume() {
                httpClientResponse.resume();
                return this;
            }

            @Override
            public ReadStream<Buffer> fetch(long amount) {
                httpClientResponse.fetch(amount);
                return this;
            }

            @Override
            public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
                if (ended.get()) {
                    if (endHandler != null) {
                        endHandler.handle(null);
                    }
                } else {
                    handlerRef.set(endHandler);
                }
                return this;
            }
        };
    }

    static EndableReadStream<Buffer> from(HttpServerRequest httpServerRequest) {

        return new EndableReadStream<Buffer>() {

            private final Logger LOGGER = LoggerFactory.getLogger(EndableReadStream.class);

            @Override
            public boolean isEnded() {
                return httpServerRequest.isEnded();
            }

            @Override
            public EndableReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
                httpServerRequest.exceptionHandler(handler);
                return this;
            }

            @Override
            public EndableReadStream<Buffer> handler(Handler<Buffer> handler) {
                httpServerRequest.handler(handler);
                return this;
            }

            @Override
            public EndableReadStream<Buffer> pause() {
                httpServerRequest.pause();
                return this;
            }

            @Override
            public EndableReadStream<Buffer> resume() {
                httpServerRequest.resume();
                return this;
            }

            @Override
            public EndableReadStream<Buffer> fetch(long amount) {
                httpServerRequest.fetch(amount);
                return this;
            }

            @Override
            public EndableReadStream<Buffer> endHandler(Handler<Void> endHandler) {
                httpServerRequest.endHandler(new Handler<Void>() {
                    @Override
                    public void handle(Void event) {
                        LOGGER.debug("End Handler Called {}", httpServerRequest.absoluteURI());
                        if (endHandler != null) {
                            LOGGER.debug("Handling end {} {}", httpServerRequest.absoluteURI(), endHandler);
                            endHandler.handle(null);
                        }
                    }
                });
                return this;
            }
        };
    }

    boolean isEnded();

}
