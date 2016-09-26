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

package org.sfs.jobs;


import com.google.common.base.Optional;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.nodes.AssignUnassignedDocumentsToNode;
import org.sfs.elasticsearch.object.MaintainObjectsForNode;
import org.sfs.nodes.Nodes;
import org.sfs.rx.AsyncResultMemoizeHandler;
import org.sfs.rx.ResultMemoizeHandler;
import org.sfs.rx.SingleAsyncResultSubscriber;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.parseLong;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.elasticsearch.AbstractBulkUpdateEndableWriteStream.BulkUpdateFailedException;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.ExceptionHelper.containsException;
import static org.sfs.util.JsonHelper.getField;
import static rx.Observable.create;
import static rx.Observable.defer;
import static rx.Observable.from;
import static rx.Observable.using;

public class Jobs {

    private static final Logger LOGGER = getLogger(Jobs.class);
    private final long initialInterval = SECONDS.toMillis(10);
    private long interval;
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean running = new AtomicBoolean(false);

    public Observable<Void> start(VertxContext<Server> vertxContext, JsonObject config) {
        return empty()
                .filter(aVoid -> started.compareAndSet(false, true))
                .flatMap(new Func1<Void, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(Void aVoid) {
                        interval = parseLong(getField(config, "jobs.maintenance_interval", valueOf(MINUTES.toMillis(5))));
                        if (LOGGER.isInfoEnabled()) {
                            LOGGER.info("Running initial maintenance sweep in " + initialInterval + "ms. Subsequent sweeps every every " + interval + "ms.");
                        }
                        Handler<Long> handler = new Handler<Long>() {

                            Handler<Long> _this = this;

                            @Override
                            public void handle(Long event) {
                                if (started.get()) {
                                    run(vertxContext)
                                            .subscribe(new Subscriber<Void>() {
                                                @Override
                                                public void onCompleted() {
                                                    if (started.get()) {
                                                        vertxContext.vertx().setTimer(interval, _this);
                                                    }
                                                }

                                                @Override
                                                public void onError(Throwable e) {
                                                    LOGGER.warn("Handling Exception", e);
                                                    if (started.get()) {
                                                        if (containsException(BulkUpdateFailedException.class, e)
                                                                || containsException(ClosedChannelException.class, e)) {
                                                            vertxContext.vertx().setTimer(initialInterval, _this);
                                                        } else {
                                                            vertxContext.vertx().setTimer(interval, _this);
                                                        }
                                                    }
                                                }

                                                @Override
                                                public void onNext(Void aVoid) {

                                                }
                                            });
                                }

                            }
                        };
                        if (started.get()) {
                            vertxContext.vertx().setTimer(initialInterval, handler);
                        }
                        return empty();
                    }
                })
                .singleOrDefault(null);
    }

    public Observable<Void> stop(VertxContext<Server> vertxContext) {
        return empty()
                .filter(aVoid -> started.compareAndSet(true, false))
                .flatMap(aVoid -> {
                    ResultMemoizeHandler<Void> handler = new ResultMemoizeHandler<>();
                    waitForStop0(vertxContext, handler);
                    return create(handler.subscribe);
                })
                .singleOrDefault(null);
    }

    protected void waitForStop0(VertxContext<Server> vertxContext, Handler<Void> handler) {
        if (!running.get()) {
            vertxContext.vertx().runOnContext(event -> handler.handle(null));
        } else {
            vertxContext.vertx().runOnContext(event -> waitForStop0(vertxContext, handler));
        }
    }

    public Observable<Void> run(VertxContext<Server> vertxContext) {
        return defer(() -> {
            AsyncResultMemoizeHandler<Void, Void> handler = new AsyncResultMemoizeHandler<>();
            run0(vertxContext, handler);
            return create(handler.subscribe);
        });
    }

    protected void run0(VertxContext<Server> vertxContext, AsyncResultHandler<Void> handler) {
        using(
                () -> running.compareAndSet(false, true),
                success -> just(success)
                        .filter(aSuccess -> aSuccess)
                        .flatMap(aVoid1 -> masterNodeSweep(vertxContext))
                        .flatMap(aVoid1 -> dataNodeSweep(vertxContext))
                        .singleOrDefault(null),
                success -> {
                    if (success) {
                        checkState(running.compareAndSet(true, false), "Concurrent Update");
                    } else {
                        throw new JobsAlreadyRunningException();
                    }
                })
                .subscribe(new SingleAsyncResultSubscriber<>(handler));
    }

    protected Observable<Void> dataNodeSweep(VertxContext<Server> vertxContext) {
        return empty()
                .filter(aVoid -> {
                    // only data nodes manage data
                    return vertxContext.verticle().nodes().isMaster();
                })
                .flatMap(new MaintainObjectsForNode(vertxContext, vertxContext.verticle().nodes().getNodeId()))
                .singleOrDefault(null);
    }

    protected Observable<Void> masterNodeSweep(VertxContext<Server> vertxContext) {
        return empty()
                .filter(aVoid -> {
                    // only data nodes manage data
                    return vertxContext.verticle().nodes().isMaster();
                })
                .flatMap(aVoid -> assignUnassignedDocumentsToNodes(vertxContext))
                .singleOrDefault(null);
    }

    protected Observable<Void> assignUnassignedDocumentsToNodes(VertxContext<Server> vertxContext) {
        return empty()
                .flatMap(aVoid -> {
                    Nodes nodes = vertxContext.verticle().nodes();
                    return from(nodes.getDataNodes(vertxContext));
                })
                .reduce(new HashMap<String, Long>(), (documentCountsByNode, persistentServiceDef) -> {
                    Optional<Long> documentCount = persistentServiceDef.getDocumentCount();
                    if (documentCount.isPresent()) {
                        documentCountsByNode.put(persistentServiceDef.getId(), documentCount.get());
                    }
                    return documentCountsByNode;
                })
                .filter(stringLongHashMap -> !stringLongHashMap.isEmpty())
                .flatMap(documentCountsByNode ->
                        empty()
                                .flatMap(new AssignUnassignedDocumentsToNode(vertxContext, documentCountsByNode)))
                .singleOrDefault(null);
    }

    public static class JobsAlreadyRunningException extends RuntimeException {

    }
}

