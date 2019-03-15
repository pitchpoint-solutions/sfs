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
import io.vertx.core.logging.Logger;
import org.sfs.util.AtomicIntMap;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class WriteQueueSupport<A> {

    private static final Logger LOGGER = getLogger(WriteQueueSupport.class);
    private final long maxWrites;
    private final long lowWater;
    private final AtomicIntMap<A> queues = new AtomicIntMap<>();
    private final ConcurrentMap<A, CopyOnWriteArrayList<ContextHandler>> writeQueueEmptyHandlers = new ConcurrentHashMap<>();
    private final Set<ContextHandler> drainHandlers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Object mutex = new Object();

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
        LinkedList<ContextHandler> toNotify = new LinkedList<>();
        synchronized (mutex) {
            drainHandlers.add(new ContextHandler(context, handler));
            notifyDrainHandlers(toNotify);
        }
        for (ContextHandler contextHandler : toNotify) {
            contextHandler.dispatch();
        }
    }

    public WriteQueueSupport<A> remove(A writer) {
        queues.remove(writer);
        LinkedList<ContextHandler> toNotify = new LinkedList<>();
        synchronized (mutex) {
            notifyDrainHandlers(toNotify);
            notifyEmptyHandlers(writer, toNotify);
        }
        for (ContextHandler contextHandler : toNotify) {
            contextHandler.dispatch();
        }
        return this;
    }

    public boolean writeQueueFull() {
        return queues.sum() >= maxWrites;
    }

    public void incrementWritesOutstanding(A writer, int delta) {
        queues.addAndGet(writer, delta);
    }

    public void decrementWritesOutstanding(A writer, int delta) {
        LinkedList<ContextHandler> toNotify = new LinkedList<>();
        queues.addAndGet(writer, -delta);
        queues.removeIfLteZero(writer);
        synchronized (mutex) {
            notifyDrainHandlers(toNotify);
            notifyEmptyHandlers(writer, toNotify);
        }
        for (ContextHandler contextHandler : toNotify) {
            contextHandler.dispatch();
        }
    }

    public void emptyHandler(A writer, Context context, Handler<Void> handler) {
        ContextHandler contextHandler = new ContextHandler(context, handler);
        CopyOnWriteArrayList<ContextHandler> existing = writeQueueEmptyHandlers.get(writer);
        if (existing == null) {
            existing = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<ContextHandler> current = writeQueueEmptyHandlers.putIfAbsent(writer, existing);
            if (current != null) {
                existing = current;
            }
        }
        existing.add(contextHandler);
        LinkedList<ContextHandler> toNotify = new LinkedList<>();
        synchronized (mutex) {
            notifyEmptyHandlers(writer, toNotify);
        }
        for (ContextHandler h : toNotify) {
            h.dispatch();
        }
    }

    public boolean writeQueueEmpty(A writer) {
        return queues.get(writer) <= 0;
    }

    public long getSize() {
        return queues.sum();
    }

    protected void notifyEmptyHandlers(A writer, LinkedList<ContextHandler> toNotify) {
        if (queues.get(writer) <= 0) {
            CopyOnWriteArrayList<ContextHandler> list = writeQueueEmptyHandlers.remove(writer);
            if (list != null) {
                toNotify.addAll(list);
            }
        }
    }

    protected void notifyDrainHandlers(LinkedList<ContextHandler> toNotify) {
        int size = queues.sum();
        if (size <= lowWater) {
            Iterator<ContextHandler> iterator = drainHandlers.iterator();
            while (iterator.hasNext()) {
                ContextHandler contextHandler = iterator.next();
                iterator.remove();
                toNotify.add(contextHandler);
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
            context.runOnContext(event -> {
                if (handler != null) {
                    handler.handle(null);
                }
            });
        }
    }
}
