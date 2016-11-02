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

package org.sfs.rx;

import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsVertx;
import org.sfs.VertxContext;
import rx.Observable;
import rx.functions.Func1;

import java.util.Queue;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class WaitForEmptyQueue implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(WaitForEmptyQueue.class);
    private final Queue<?> queue;
    private final SfsVertx vertx;

    public WaitForEmptyQueue(VertxContext<Server> vertxContext, Queue<?> queue) {
        this(vertxContext.vertx(), queue);
    }

    public WaitForEmptyQueue(SfsVertx vertx, Queue<?> queue) {
        this.vertx = vertx;
        this.queue = queue;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        ObservableFuture<Void> handler = RxHelper.observableFuture();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Waiting for empty queue. Size is " + queue.size());
        }
        if (queue.size() > 0) {
            vertx.setPeriodic(20, event -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Waiting for empty queue. Size is " + queue.size());
                }
                if (queue.size() <= 0) {
                    vertx.cancelTimer(event);
                    handler.complete(null);
                }
            });
        } else {
            handler.complete(null);
        }
        return handler
                .map(aVoid1 -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Done waiting for empty queue. Size is " + queue.size());
                    }
                    return null;
                });
    }
}
