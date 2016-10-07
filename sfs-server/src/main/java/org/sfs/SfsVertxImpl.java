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


import io.netty.channel.EventLoopGroup;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.TimeoutStream;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.datagram.DatagramSocketOptions;
import io.vertx.core.dns.DnsClient;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.shareddata.SharedData;
import io.vertx.core.spi.VerticleFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func0;

import java.util.Set;
import java.util.concurrent.ExecutorService;

public class SfsVertxImpl implements SfsVertx {

    private static Logger LOGGER = LoggerFactory.getLogger(SfsVertxImpl.class);
    private final Vertx vertx;
    private final ExecutorService backgroundPool;
    private final ExecutorService ioPool;

    public SfsVertxImpl(Vertx vertx, ExecutorService backgroundPool, ExecutorService ioPool) {
        this.vertx = vertx;
        this.backgroundPool = backgroundPool;
        this.ioPool = ioPool;
    }

    @Override
    public ExecutorService getIoPool() {
        return ioPool;
    }

    @Override
    public ExecutorService getBackgroundPool() {
        return backgroundPool;
    }

    @Override
    public <T> void executeBlocking(Func0<T> action, Handler<AsyncResult<T>> asyncResultHandler) {
        Context context = getOrCreateContext();
        backgroundPool.execute(() -> {
            T resultr = null;
            try {
                T result = action.call();
                resultr = result;
                context.runOnContext(event -> Future.succeededFuture(result).setHandler(asyncResultHandler));
            } catch (Throwable e) {
                LOGGER.error("Result was " + resultr + ", Context was " + context + ", Handler was " + asyncResultHandler, e);
                context.runOnContext(event -> Future.<T>failedFuture(e).setHandler(asyncResultHandler));
            }
        });
    }

    @Override
    public <T> void executeBlockingObservable(Func0<Observable<T>> action, Handler<AsyncResult<T>> asyncResultHandler) {
        Context context = getOrCreateContext();
        backgroundPool.execute(() -> {
            try {
                Observable<T> o = action.call();
                o.single()
                        .subscribe(new Subscriber<T>() {

                            T result;

                            @Override
                            public void onCompleted() {
                                context.runOnContext(event -> Future.succeededFuture(result).setHandler(asyncResultHandler));
                            }

                            @Override
                            public void onError(Throwable e) {
                                context.runOnContext(event -> Future.<T>failedFuture(e).setHandler(asyncResultHandler));
                            }

                            @Override
                            public void onNext(T t) {
                                result = t;
                            }
                        });
            } catch (Throwable e) {
                context.runOnContext(event -> Future.<T>failedFuture(e).setHandler(asyncResultHandler));
            }
        });
    }

    @Override
    public boolean cancelTimer(long id) {
        return vertx.cancelTimer(id);
    }

    @Override
    public Context getOrCreateContext() {
        return vertx.getOrCreateContext();
    }

    @Override
    public NetServer createNetServer(NetServerOptions options) {
        return vertx.createNetServer(options);
    }

    @Override
    public NetServer createNetServer() {
        return vertx.createNetServer();
    }

    @Override
    public NetClient createNetClient(NetClientOptions options) {
        return vertx.createNetClient(options);
    }

    @Override
    public NetClient createNetClient() {
        return vertx.createNetClient();
    }

    @Override
    public HttpServer createHttpServer(HttpServerOptions options) {
        return vertx.createHttpServer(options);
    }

    @Override
    public HttpServer createHttpServer() {
        return vertx.createHttpServer();
    }

    @Override
    public HttpClient createHttpClient(HttpClientOptions options) {
        return vertx.createHttpClient(options);
    }

    @Override
    public HttpClient createHttpClient() {
        return vertx.createHttpClient();
    }

    @Override
    public DatagramSocket createDatagramSocket(DatagramSocketOptions options) {
        return vertx.createDatagramSocket(options);
    }

    @Override
    public DatagramSocket createDatagramSocket() {
        return vertx.createDatagramSocket();
    }

    @Override
    public FileSystem fileSystem() {
        return vertx.fileSystem();
    }

    @Override
    public EventBus eventBus() {
        return vertx.eventBus();
    }

    @Override
    public DnsClient createDnsClient(int port, String host) {
        return vertx.createDnsClient(port, host);
    }

    @Override
    public SharedData sharedData() {
        return vertx.sharedData();
    }

    @Override
    public long setTimer(long delay, Handler<Long> handler) {
        return vertx.setTimer(delay, handler);
    }

    @Override
    public TimeoutStream timerStream(long delay) {
        return vertx.timerStream(delay);
    }

    @Override
    public long setPeriodic(long delay, Handler<Long> handler) {
        return vertx.setPeriodic(delay, handler);
    }

    @Override
    public TimeoutStream periodicStream(long delay) {
        return vertx.periodicStream(delay);
    }

    @Override
    public void runOnContext(Handler<Void> action) {
        vertx.runOnContext(action);
    }

    @Override
    public void close() {
        vertx.close();
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        vertx.close(completionHandler);
    }

    @Override
    public void deployVerticle(Verticle verticle) {
        vertx.deployVerticle(verticle);
    }

    @Override
    public void deployVerticle(Verticle verticle, Handler<AsyncResult<String>> completionHandler) {
        vertx.deployVerticle(verticle, completionHandler);
    }

    @Override
    public void deployVerticle(Verticle verticle, DeploymentOptions options) {
        vertx.deployVerticle(verticle, options);
    }

    @Override
    public void deployVerticle(Verticle verticle, DeploymentOptions options, Handler<AsyncResult<String>> completionHandler) {
        vertx.deployVerticle(verticle, options, completionHandler);
    }

    @Override
    public void deployVerticle(String name) {
        vertx.deployVerticle(name);
    }

    @Override
    public void deployVerticle(String name, Handler<AsyncResult<String>> completionHandler) {
        vertx.deployVerticle(name, completionHandler);
    }

    @Override
    public void deployVerticle(String name, DeploymentOptions options) {
        vertx.deployVerticle(name, options);
    }

    @Override
    public void deployVerticle(String name, DeploymentOptions options, Handler<AsyncResult<String>> completionHandler) {
        vertx.deployVerticle(name, options, completionHandler);
    }

    @Override
    public void undeploy(String deploymentID) {
        vertx.undeploy(deploymentID);
    }

    @Override
    public void undeploy(String deploymentID, Handler<AsyncResult<Void>> completionHandler) {
        vertx.undeploy(deploymentID, completionHandler);
    }

    @Override
    public Set<String> deploymentIDs() {
        return vertx.deploymentIDs();
    }

    @Override
    public void registerVerticleFactory(VerticleFactory factory) {
        vertx.registerVerticleFactory(factory);
    }

    @Override
    public void unregisterVerticleFactory(VerticleFactory factory) {
        vertx.unregisterVerticleFactory(factory);
    }

    @Override
    public Set<VerticleFactory> verticleFactories() {
        return vertx.verticleFactories();
    }

    @Override
    public boolean isClustered() {
        return vertx.isClustered();
    }

    @Override
    public <T> void executeBlocking(Handler<Future<T>> blockingCodeHandler, boolean ordered, Handler<AsyncResult<T>> asyncResultHandler) {
        vertx.executeBlocking(blockingCodeHandler, ordered, asyncResultHandler);
    }

    @Override
    public <T> void executeBlocking(Handler<Future<T>> blockingCodeHandler, Handler<AsyncResult<T>> asyncResultHandler) {
        vertx.executeBlocking(blockingCodeHandler, asyncResultHandler);
    }

    @Override
    public EventLoopGroup nettyEventLoopGroup() {
        return vertx.nettyEventLoopGroup();
    }

    @Override
    public WorkerExecutor createSharedWorkerExecutor(String name) {
        return vertx.createSharedWorkerExecutor(name);
    }

    @Override
    public WorkerExecutor createSharedWorkerExecutor(String name, int poolSize) {
        return vertx.createSharedWorkerExecutor(name, poolSize);
    }

    @Override
    public WorkerExecutor createSharedWorkerExecutor(String name, int poolSize, long maxExecuteTime) {
        return vertx.createSharedWorkerExecutor(name, poolSize, maxExecuteTime);
    }

    @Override
    public Vertx exceptionHandler(Handler<Throwable> handler) {
        return vertx.exceptionHandler(handler);
    }

    @Override
    public Handler<Throwable> exceptionHandler() {
        return vertx.exceptionHandler();
    }

    @Override
    public boolean isMetricsEnabled() {
        return vertx.isMetricsEnabled();
    }
}
