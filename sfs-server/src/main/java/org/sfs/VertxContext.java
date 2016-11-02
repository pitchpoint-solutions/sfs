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

package org.sfs;


import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.RxVertx;
import rx.Observable;
import rx.functions.Func0;

import java.util.concurrent.ExecutorService;

import static rx.Observable.defer;

public class VertxContext<A extends Server> {

    private final A verticle;
    private final SfsVertx vertx;
    private final RxVertx rxVertx;
    private final ExecutorService backgroundPool;
    private final ExecutorService ioPool;

    public VertxContext(A verticle) {
        this.verticle = verticle;
        this.rxVertx = new RxVertx(verticle.getVertx());
        this.backgroundPool = verticle.getBackgroundPool();
        this.ioPool = verticle.getIoPool();
        this.vertx = new SfsVertxImpl(verticle.getVertx(), backgroundPool, ioPool);
    }

    public A verticle() {
        return verticle;
    }

    public SfsVertx vertx() {
        return vertx;
    }

    public RxVertx rxVertx() {
        return rxVertx;
    }

    public ExecutorService backgroundPool() {
        return backgroundPool;
    }

    public ExecutorService getIoPool() {
        return ioPool;
    }

    public <R> Observable<R> executeBlocking(final Func0<R> func) {
        return executeBlocking(vertx, func);
    }

    public <R> Observable<R> executeBlockingObservable(final Func0<Observable<R>> func) {
        return executeBlockingObservable(vertx, func);
    }

    public static <R> Observable<R> executeBlocking(SfsVertx vertx, Func0<R> func) {
        return defer(() -> {
            ObservableFuture<R> handler = RxHelper.observableFuture();
            vertx.executeBlocking(func, handler.toHandler());
            return handler;
        });
    }

    public static <R> Observable<R> executeBlockingObservable(SfsVertx vertx, Func0<Observable<R>> func) {
        return defer(() -> {
            ObservableFuture<R> handler = RxHelper.observableFuture();
            vertx.executeBlockingObservable(func::call, handler.toHandler());
            return handler;
        });
    }
}
