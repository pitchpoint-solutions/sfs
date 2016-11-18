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


import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.sfs.util.AtomicIntMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class WriteQueueSupport<A> {

    private final long maxWrites;
    private final long lowWater;
    private final AtomicIntMap<A> queues = new AtomicIntMap<>();
    private final ConcurrentMap<A, ContextHandlerList> writeQueueEmptyHandlers = new ConcurrentHashMap<>();
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

    public void drainHandler(Context context, Handler<Void> handler) {
        drainHandlers.add(new ContextHandler(context, handler));
        notifyDrainHandlers();
    }

    public WriteQueueSupport<A> remove(A entity) {
        queues.remove(entity);
        notifyDrainHandlers();
        notifyEmptyHandlers(entity);
        return this;
    }

    public boolean writeQueueFull() {
        return queues.sum() >= maxWrites;
    }

    public void incrementWritesOutstanding(A writer, int delta) {
        queues.addAndGet(writer, delta);
    }

    public void decrementWritesOutstanding(A writer, int delta) {
        queues.addAndGet(writer, -delta);
        queues.removeIfLteZero(writer);
        notifyDrainHandlers();
        notifyEmptyHandlers(writer);
    }

    public void emptyHandler(A writer, Context context, Handler<Void> handler) {
        ContextHandler contextHandler = new ContextHandler(context, handler);
        while (true) {
            ContextHandlerList existing = writeQueueEmptyHandlers.get(writer);
            if (existing != null) {
                ContextHandlerList updated = new ContextHandlerList(new ArrayList<>(existing.getList()));
                updated.add(contextHandler);
                if (writeQueueEmptyHandlers.replace(writer, existing, updated)) {
                    break;
                }
            } else {
                ContextHandlerList updated = new ContextHandlerList(Collections.singletonList(contextHandler));
                if (writeQueueEmptyHandlers.putIfAbsent(writer, updated) == null) {
                    break;
                }
            }
        }
        notifyEmptyHandlers(writer);
    }

    public boolean writeQueueEmpty(A writer) {
        return queues.get(writer) <= 0;
    }

    public long getSize() {
        return queues.sum();
    }

    protected void notifyEmptyHandlers(A writer) {
        if (queues.get(writer) <= 0) {
            ContextHandlerList contextHandlerList = writeQueueEmptyHandlers.remove(writer);
            if (contextHandlerList != null) {
                for (ContextHandler contextHandler : contextHandlerList.getList()) {
                    contextHandler.dispatch();
                }
            }
        }
    }

    protected void notifyDrainHandlers() {
        int size = queues.sum();
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

    private static class ContextHandlerList {

        private final List<ContextHandler> list;

        public ContextHandlerList(List<ContextHandler> list) {
            this.list = list;
        }

        public List<ContextHandler> getList() {
            return list;
        }

        public void add(ContextHandler contextHandler) {
            list.add(contextHandler);
        }
    }
}
