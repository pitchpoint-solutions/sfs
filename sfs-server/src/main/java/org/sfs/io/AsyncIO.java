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
import io.vertx.core.file.AsyncFile;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.WriteStream;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;

import static io.vertx.core.logging.LoggerFactory.getLogger;


public class AsyncIO {

    private static final Logger LOGGER = getLogger(AsyncIO.class);

    public static Observable<Void> append(Buffer buffer, final WriteStream<Buffer> ws) {
        try {

            ObservableFuture<Void> drainHandler = RxHelper.observableFuture();
            ws.exceptionHandler(drainHandler::fail);
            if (ws.writeQueueFull()) {
                ws.drainHandler(drainHandler::complete);
            } else {
                drainHandler.complete(null);
            }

            return drainHandler.flatMap(aVoid -> {
                ObservableFuture<Void> writeHandler = RxHelper.observableFuture();
                ws.exceptionHandler(writeHandler::fail);
                ws.write(buffer);
                if (ws.writeQueueFull()) {
                    ws.drainHandler(writeHandler::complete);
                } else {
                    writeHandler.complete(null);
                }
                return writeHandler;
            });
        } catch (Throwable e) {
            return Observable.error(e);
        }

    }

    public static Observable<Void> end(Buffer buffer, final BufferEndableWriteStream ws) {
        try {
            ObservableFuture<Void> drainHandler = RxHelper.observableFuture();
            ws.exceptionHandler(drainHandler::fail);
            if (ws.writeQueueFull()) {
                ws.drainHandler(drainHandler::complete);
            } else {
                drainHandler.complete(null);
            }
            return drainHandler.flatMap(aVoid -> {
                ObservableFuture<Void> endHandler = RxHelper.observableFuture();
                ws.exceptionHandler(endHandler::fail);
                ws.endHandler(endHandler::complete);
                ws.end(buffer);
                return endHandler;
            });
        } catch (Throwable e) {
            return Observable.error(e);
        }
    }

    public static Observable<Void> append(Buffer buffer, final BufferEndableWriteStream ws) {
        try {
            ObservableFuture<Void> drainHandler = RxHelper.observableFuture();
            ws.exceptionHandler(drainHandler::fail);
            if (ws.writeQueueFull()) {
                ws.drainHandler(drainHandler::complete);
            } else {
                drainHandler.complete(null);
            }

            return drainHandler.flatMap(aVoid -> {
                ObservableFuture<Void> writeHandler = RxHelper.observableFuture();
                ws.exceptionHandler(writeHandler::fail);
                ws.write(buffer);
                if (ws.writeQueueFull()) {
                    ws.drainHandler(writeHandler::complete);
                } else {
                    writeHandler.complete(null);
                }
                return writeHandler;
            });
        } catch (Throwable e) {
            return Observable.error(e);
        }
    }

    public static Observable<Void> pump(final HttpClientResponse rs, final EndableWriteStream<Buffer> ws) {
        return pump(EndableReadStream.from(rs), ws);
    }

    public static Observable<Void> pump(final HttpServerRequest rs, final EndableWriteStream<Buffer> ws) {
        return pump(EndableReadStream.from(rs), ws);
    }

    public static <M> Observable<Void> pump(final EndableReadStream<M> rs, final EndableWriteStream<M> ws) {
        try {
            rs.pause();
            ObservableFuture<Void> observableFuture = RxHelper.observableFuture();

            Handler<Throwable> exceptionHandler = observableFuture::fail;

            rs.exceptionHandler(exceptionHandler);
            ws.exceptionHandler(exceptionHandler);

            Handler<Void> drainHandler = event -> rs.resume();

            Handler<M> dataHandler = data -> {
                ws.write(data);
                if (ws.writeQueueFull()) {
                    rs.pause();
                    ws.drainHandler(drainHandler);
                }
            };
            ws.endHandler(observableFuture::complete);
            if (rs.isEnded()) {
                rs.resume();
                ws.end();
            } else {
                rs.endHandler(event -> ws.end());
                rs.handler(dataHandler);
                rs.resume();
            }
            return observableFuture;
        } catch (Throwable e) {
            return Observable.error(e);
        }
    }

    public static Observable<Void> close(AsyncFile asyncFile) {
        try {
            ObservableFuture<Void> rh = RxHelper.observableFuture();
            asyncFile.close(rh.toHandler());
            return rh;
        } catch (Throwable e) {
            return Observable.error(e);
        }
    }
}
