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

package org.sfs.jobs;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.ContextHandler;
import org.sfs.rx.Defer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.util.ExceptionHelper;
import rx.Observable;
import rx.Scheduler;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractJob implements Job {


    private final Set<ContextHandler<Void>> waiters = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private AtomicBoolean complete = new AtomicBoolean(false);

    @Override
    public Observable<Void> waitStopped(VertxContext<Server> vertxContext, long timeout, TimeUnit timeUnit) {
        Vertx vertx = vertxContext.vertx();
        Scheduler scheduler = RxHelper.scheduler(vertx.getOrCreateContext());
        ObservableFuture<Void> handler = RxHelper.observableFuture();
        ContextHandler<Void> waiter = new ContextHandler<>(vertx, handler::complete, handler::fail);
        waiters.add(waiter);
        return Defer.aVoid()
                .doOnNext(aVoid -> waiters.add(waiter))
                .doOnNext(aVoid -> notifyComplete())
                .flatMap(aVoid -> handler)
                .timeout(timeout, timeUnit, scheduler)
                .doOnError(throwable -> waiters.remove(waiter))
                .onErrorResumeNext(throwable -> {
                    if (ExceptionHelper.containsException(TimeoutException.class, throwable)) {
                        return Observable.error(new Jobs.WaitStoppedExpired());
                    } else {
                        return Observable.error(throwable);
                    }
                });
    }

    @Override
    public Observable<Void> waitStopped(VertxContext<Server> vertxContext) {
        Vertx vertx = vertxContext.vertx();
        ObservableFuture<Void> handler = RxHelper.observableFuture();
        ContextHandler<Void> waiter = new ContextHandler<>(vertx, handler::complete, handler::fail);
        return Defer.aVoid()
                .doOnNext(aVoid -> waiters.add(waiter))
                .doOnNext(aVoid -> notifyComplete())
                .flatMap(aVoid -> handler)
                .doOnError(throwable -> waiters.remove(waiter));
    }

    @Override
    public Observable<Void> execute(VertxContext<Server> vertxContext, MultiMap parameters) {
        return executeImpl(vertxContext, parameters)
                .single()
                .doOnNext(aVoid -> complete.set(true))
                .doOnNext(aVoid -> notifyComplete())
                .doOnError(this::notifyException);
    }

    protected abstract Observable<Void> executeImpl(VertxContext<Server> vertxContext, MultiMap parameters);

    public Observable<Void> stop(VertxContext<Server> vertxContext) {
        return stopImpl(vertxContext)
                .doOnError(this::notifyException);
    }

    protected abstract Observable<Void> stopImpl(VertxContext<Server> vertxContext);

    private void notifyException(Throwable e) {
        Iterator<ContextHandler<Void>> iterator = waiters.iterator();
        while (iterator.hasNext()) {
            ContextHandler<Void> next = iterator.next();
            iterator.remove();
            next.fail(e);
        }
    }

    private void notifyComplete() {
        if (complete.get()) {
            Iterator<ContextHandler<Void>> iterator = waiters.iterator();
            while (iterator.hasNext()) {
                ContextHandler<Void> next = iterator.next();
                iterator.remove();
                next.complete(null);
            }
        }
    }
}
