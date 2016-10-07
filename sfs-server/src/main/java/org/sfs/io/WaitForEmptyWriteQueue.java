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

import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsVertx;
import org.sfs.VertxContext;
import org.sfs.rx.MemoizeHandler;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static rx.Observable.create;

public class WaitForEmptyWriteQueue implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(WaitForEmptyWriteQueue.class);
    private final WriteQueueSupport writeQueueSupport;
    private final SfsVertx vertx;

    public WaitForEmptyWriteQueue(VertxContext<Server> vertxContext, WriteQueueSupport writeQueueSupport) {
        this(vertxContext.vertx(), writeQueueSupport);
    }

    public WaitForEmptyWriteQueue(SfsVertx vertx, WriteQueueSupport writeQueueSupport) {
        this.vertx = vertx;
        this.writeQueueSupport = writeQueueSupport;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        final MemoizeHandler<Void, Void> handler = new MemoizeHandler<>();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Waiting for empty write queue. Size is " + writeQueueSupport.getSize());
        }
        if (writeQueueSupport.getSize() > 0) {
            vertx.setPeriodic(20, event -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Waiting for empty write queue. Size is " + writeQueueSupport.getSize());
                }
                if (writeQueueSupport.getSize() <= 0) {
                    vertx.cancelTimer(event);
                    handler.complete(null);
                }
            });
        } else {
            handler.complete(null);
        }
        return create(handler.subscribe)
                .map(aVoid1 -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Done waiting for empty write queue. Size is " + writeQueueSupport.getSize());
                    }
                    return null;
                });
    }
}
