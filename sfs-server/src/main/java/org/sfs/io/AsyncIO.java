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
import io.vertx.core.file.AsyncFile;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.AsyncResultMemoizeHandler;
import org.sfs.rx.Defer;
import org.sfs.rx.ResultMemoizeHandler;
import rx.Observable;

import java.nio.channels.AsynchronousFileChannel;


public class AsyncIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncIO.class);

    public static Observable<Void> append(Buffer buffer, final WriteStream<Buffer> ws) {
        return Observable.defer(() -> {
            final ResultMemoizeHandler<Void> handler = new ResultMemoizeHandler<>();
            ws.exceptionHandler(handler::fail);
            ws.write(buffer);
            if (ws.writeQueueFull()) {
                ws.drainHandler(handler);
            } else {
                handler.handle(null);
            }
            return Observable.create(handler.subscribe);
        });
    }

    public static Observable<Void> end(Buffer buffer, final BufferEndableWriteStream ws) {
        return Observable.defer(() -> {
            BufferReadStream bufferReadStream = new BufferReadStream(buffer);
            return pump(bufferReadStream, ws);
        });
    }

    public static Observable<Void> append(Buffer buffer, final BufferEndableWriteStream ws) {
        return Observable.defer(() -> {
            final ResultMemoizeHandler<Void> handler = new ResultMemoizeHandler<>();
            ws.exceptionHandler(handler::fail);
            ws.write(buffer);
            if (ws.writeQueueFull()) {
                ws.drainHandler(event -> handler.complete(null));
            } else {
                handler.complete(null);
            }
            return Observable.create(handler.subscribe);
        });
    }

    public static <M> Observable<Void> pump(final ReadStream<M> rs, final EndableWriteStream<M> ws) {
        rs.pause();
        return Observable.defer(() -> {
            ResultMemoizeHandler<Void> handler = new ResultMemoizeHandler<>();
            Handler<Throwable> exceptionHandler = throwable -> {
                try {
                    ws.drainHandler(null);
                    rs.handler(null);
                } catch (Throwable e) {
                    handler.fail(e);
                    return;
                }
                handler.fail(throwable);
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
            ws.endHandler(event -> handler.complete(null));
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
            return Observable.create(handler.subscribe);
        });
    }


    public static Observable<Void> close(AsyncFile asyncFile) {
        return Observable.defer(() -> {
            AsyncResultMemoizeHandler<Void, Void> rh = new AsyncResultMemoizeHandler<>();
            asyncFile.close(rh);
            return Observable.create(rh.subscribe);
        });
    }

    public static Observable<Void> close(VertxContext<Server> vertxContext, final WriteQueueSupport writeQueueSupport, final AsynchronousFileChannel channel) {
        if (channel != null) {
            return Defer.empty()
                    .flatMap(new WaitForEmptyWriteQueue(vertxContext, writeQueueSupport))
                    .flatMap(aVoid -> vertxContext.executeBlocking(() -> {
                        try {
                            channel.force(true);
                            channel.close();
                        } catch (Throwable e) {
                            LOGGER.warn("Unhandled Exception", e);
                        }
                        return null;
                    }));
        } else {
            return Defer.empty();
        }
    }
}
