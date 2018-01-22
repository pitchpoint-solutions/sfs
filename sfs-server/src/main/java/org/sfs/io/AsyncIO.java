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
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;


public class AsyncIO {

    public static Observable<Void> append(Buffer buffer, final WriteStream<Buffer> ws) {
        return Observable.defer(() -> {

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
        });
    }

    public static Observable<Void> end(Buffer buffer, final BufferEndableWriteStream ws) {
        return Observable.defer(() -> {
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
        });
    }

    public static Observable<Void> append(Buffer buffer, final BufferEndableWriteStream ws) {
        return Observable.defer(() -> {
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
        });
    }

    public static <M> Observable<Void> pump(final ReadStream<M> rs, final EndableWriteStream<M> ws) {
        rs.pause();
        return Observable.defer(() -> {
            ObservableFuture<Void> observableFuture = RxHelper.observableFuture();
            Handler<Throwable> exceptionHandler = throwable -> {
                try {
                    ws.drainHandler(null);
                    rs.handler(null);
                } catch (Throwable e) {
                    observableFuture.fail(e);
                    return;
                }
                observableFuture.fail(throwable);
            };
            rs.exceptionHandler(exceptionHandler);
            ws.exceptionHandler(exceptionHandler);
            Handler<Void> drainHandler = event -> {
                try {
                    rs.resume();
                } catch (Throwable e) {
                    exceptionHandler.handle(e);
                }
            };

            Handler<M> dataHandler = data -> {
                try {
                    ws.write(data);
                    if (ws.writeQueueFull()) {
                        rs.pause();
                        ws.drainHandler(drainHandler);
                    }
                } catch (Throwable e) {
                    exceptionHandler.handle(e);
                }
            };
            ws.endHandler(observableFuture::complete);
            rs.endHandler(event -> {
                try {
                    ws.end();
                } catch (Throwable e) {
                    exceptionHandler.handle(e);
                }
            });
            try {
                rs.handler(dataHandler);
                rs.resume();
            } catch (Throwable e) {
                exceptionHandler.handle(e);
            }
            return observableFuture;
        });
    }


    public static Observable<Void> close(AsyncFile asyncFile) {
        return Observable.defer(() -> {
            ObservableFuture<Void> rh = RxHelper.observableFuture();
            asyncFile.close(rh.toHandler());
            return rh;
        });
    }

}
