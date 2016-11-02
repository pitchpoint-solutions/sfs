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


import com.google.common.util.concurrent.AtomicLongMap;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WriteQueueSupport<A> {

    private final long maxWrites;
    private final long lowWater;
    private final AtomicLongMap<A> queues = AtomicLongMap.create();
    private final Set<ContextHandler> drainHandlers = Collections.newSetFromMap(new ConcurrentHashMap<>());


    public WriteQueueSupport(long maxWrites) {
        this.maxWrites = maxWrites;
        this.lowWater = maxWrites / 2;
    }

    public long getMaxWrites() {
        return maxWrites;
    }

    public long getLowWater() {
        return lowWater;
    }

    public void drainHandler(Vertx vertx, Handler<Void> handler) {
        drainHandler(vertx.getOrCreateContext(), handler);
    }

    public void drainHandler(Context context, Handler<Void> handler) {
        drainHandlers.add(new ContextHandler(context, handler));
        notifyDrainHandlers();
    }

    public WriteQueueSupport<A> remove(A writer) {
        queues.remove(writer);
        notifyDrainHandlers();
        return this;
    }

    public boolean writeQueueFull() {
        return queues.sum() >= maxWrites;
    }

    public void incrementWritesOutstanding(A writer, long delta) {
        queues.addAndGet(writer, delta);
    }

    public boolean writeQueueEmpty(A writer) {
        return queues.get(writer) <= 0;
    }

    public long getSize() {
        return queues.sum();
    }

    protected void notifyDrainHandlers() {
        long size = queues.sum();
        if (size <= lowWater) {
            Iterator<ContextHandler> iterator = drainHandlers.iterator();
            while (iterator.hasNext()) {
                ContextHandler contextHandler = iterator.next();
                iterator.remove();
                contextHandler.dispatch();
            }
        }
    }

    private static class ContextHandler {

        private final Context context;
        private final Handler<Void> handler;

        public ContextHandler(Context context, Handler<Void> handler) {
            this.context = context;
            this.handler = handler;
        }

        public void dispatch() {
            context.runOnContext(event -> handler.handle(null));
        }
    }
}
