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

package org.sfs.nodes;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.nodes.DeleteServiceDef;
import org.sfs.elasticsearch.nodes.GetDocumentsCountForNode;
import org.sfs.elasticsearch.nodes.LoadServiceDef;
import org.sfs.elasticsearch.nodes.PersistServiceDef;
import org.sfs.elasticsearch.nodes.UpdateServiceDef;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.rx.ToVoid;
import org.sfs.vo.PersistentServiceDef;
import org.sfs.vo.TransientServiceDef;
import org.sfs.vo.TransientXFileSystem;
import org.sfs.vo.TransientXListener;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.nio.file.FileStore;
import java.nio.file.Path;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.net.HostAndPort.fromParts;
import static com.google.common.net.InetAddresses.forString;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Runtime.getRuntime;
import static java.net.NetworkInterface.getNetworkInterfaces;
import static java.nio.file.Files.getFileStore;
import static java.util.Calendar.getInstance;
import static java.util.Collections.list;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static rx.Observable.from;

public class NodeStats {

    private static Logger LOGGER = getLogger(NodeStats.class);
    private ImmutableList<TransientXListener> listeners;
    private VertxContext<Server> vertxContext;
    private boolean started = false;
    private TransientServiceDef transientServiceDef;
    private Long timerId;
    private long pingTimeout;
    private long pingInterval;

    public NodeStats(ImmutableList<TransientXListener> listeners, long pingTimeout, long pingInterval) {
        this.listeners = listeners;
        this.pingTimeout = pingTimeout;
        this.pingInterval = pingInterval;
    }

    public ImmutableList<TransientXListener> getListeners() {
        return listeners;
    }

    public long getPingTimeout() {
        return pingTimeout;
    }

    public long getPingInterval() {
        return pingInterval;
    }

    public Observable<Void> open(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
        return empty()
                .flatMap(aVoid -> generate(vertxContext))
                .flatMap(aVoid -> persist(vertxContext))
                .doOnNext(aVoid -> startTimer())
                .doOnNext(aVoid -> started = true);
    }

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        return empty()
                .doOnNext(aVoid -> started = false)
                .doOnNext(aVoid -> stopTimer())
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return empty();
                })
                .flatMap(aVoid -> remove(vertxContext))
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return empty();
                })
                .doOnNext(aVoid -> {
                    transientServiceDef = null;
                });
    }

    public Observable<Void> forceUpdate(VertxContext<Server> vertxContext) {
        return generate(vertxContext)
                .flatMap(aVoid -> persist(vertxContext));
    }

    public Optional<TransientServiceDef> getStats() {
        checkStarted();
        return fromNullable(transientServiceDef);
    }

    protected void startTimer() {
        Handler<Long> handler = new Handler<Long>() {

            Handler<Long> _this = this;

            @Override
            public void handle(Long event) {
                generate(vertxContext)
                        .flatMap(aVoid -> persist(vertxContext))
                        .subscribe(new Subscriber<Void>() {
                            @Override
                            public void onCompleted() {
                                timerId = vertxContext.vertx().setTimer(pingInterval, _this);
                            }

                            @Override
                            public void onError(Throwable e) {
                                LOGGER.debug("Handling Exception", e);
                                timerId = vertxContext.vertx().setTimer(pingInterval, _this);
                            }

                            @Override
                            public void onNext(Void aVoid1) {

                            }
                        });
            }
        };
        timerId = vertxContext.vertx().setTimer(pingInterval, handler);
    }

    protected void stopTimer() {
        if (timerId != null) {
            vertxContext.vertx().cancelTimer(timerId);
        }
    }

    protected void checkStarted() {
        checkState(started, "Not started");
    }


    protected Observable<Void> remove(VertxContext<Server> vertxContext) {
        String nodeId = vertxContext.verticle().nodes().getNodeId();
        return just(nodeId)
                .filter(s -> s != null)
                .flatMap(new LoadServiceDef(vertxContext))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(persistentServiceDef ->
                        Observable.just(persistentServiceDef)
                                .flatMap(new DeleteServiceDef(vertxContext))
                                .map(persistentServiceDefOptional -> (Void) null))
                .singleOrDefault(null);
    }

    protected Observable<Void> persist(VertxContext<Server> vertxContext) {
        String nodeId = vertxContext.verticle().nodes().getNodeId();
        return just(nodeId)
                .filter(s -> transientServiceDef != null)
                .filter(s -> nodeId != null)
                .flatMap(new LoadServiceDef(vertxContext))
                .map(persistentServiceDefOptional -> {
                    if (persistentServiceDefOptional.isPresent()) {
                        PersistentServiceDef persistentServiceDef = persistentServiceDefOptional.get();
                        return persistentServiceDef.merge(transientServiceDef)
                                .setLastUpdate(getInstance());
                    } else {
                        return transientServiceDef;
                    }
                })
                .filter(serviceDef -> serviceDef != null)
                .flatMap(serviceDef -> {
                    if (serviceDef instanceof TransientServiceDef) {
                        return Observable.just((TransientServiceDef) serviceDef)
                                .flatMap(new PersistServiceDef(vertxContext, pingTimeout));
                    } else {
                        return Observable.just((PersistentServiceDef) serviceDef)
                                .flatMap(new UpdateServiceDef(vertxContext, pingTimeout));
                    }
                })
                .map(persistentServiceDefOptional -> (Void) null)
                .singleOrDefault(null);

    }

    protected Observable<Void> generate(VertxContext<Server> vertxContext) {
        NodeStats _this = this;
        return empty()
                .flatMap(aVoid -> vertxContext.executeBlocking(() -> {
                    try {
                        Nodes nodes = vertxContext.verticle().nodes();

                        Path workingDirectory = vertxContext.verticle().sfsFileSystem().workingDirectory();
                        FileStore fileStore = getFileStore(workingDirectory);

                        TransientXFileSystem fileSystemInfo = new TransientXFileSystem()
                                .setDevice(fileStore.name())
                                .setPath(workingDirectory.toString())
                                .setTotalSpace(fileStore.getTotalSpace())
                                .setUnallocatedSpace(fileStore.getUnallocatedSpace())
                                .setUsableSpace(fileStore.getUsableSpace())
                                .setType(fileStore.type())
                                .setPartition(workingDirectory.getRoot().toString());

                        Runtime runtime = getRuntime();

                        return new TransientServiceDef(nodes.getNodeId())
                                .setAvailableProcessors(runtime.availableProcessors())
                                .setFreeMemory(runtime.freeMemory())
                                .setMaxMemory(runtime.maxMemory())
                                .setTotalMemory(runtime.totalMemory())
                                .setDataNode(nodes.isDataNode())
                                .setMaster(nodes.isMaster())
                                .setBackgroundPoolQueueSize(nodes.getBackgroundQueueSize())
                                .setIoPoolQueueSize(nodes.getIoQueueSize())
                                .setFileSystem(fileSystemInfo)
                                .setListeners(availableListeners());

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })).flatMap(transientServiceDef -> {
                    VolumeManager volumeManager = vertxContext.verticle().nodes().volumeManager();
                    if (volumeManager != null) {
                        return from(volumeManager.volumes())
                                .map(volumeManager::get)
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .flatMap(volume -> volume.volumeInfo(vertxContext.vertx()))
                                .toList()
                                .map(transientServiceDef::setVolumes);
                    } else {
                        return Observable.just(transientServiceDef);
                    }
                })
                .flatMap(transientServiceDef -> getDocumentCount()
                        .doOnNext(transientServiceDef::setDocumentCount)
                        .map(documentCount -> transientServiceDef))
                .doOnNext(transientServiceDef1 -> _this.transientServiceDef = transientServiceDef1)
                .map(new ToVoid<>());

    }

    protected Observable<Long> getDocumentCount() {
        boolean dataNode = vertxContext.verticle().nodes().isDataNode();
        String nodeId = vertxContext.verticle().nodes().getNodeId();
        return
                dataNode ?
                        empty()
                                .flatMap(new GetDocumentsCountForNode(vertxContext, nodeId))
                        : just(0L);
    }

    protected Iterable<TransientXListener> availableListeners() {
        try {
            return
                    FluentIterable.from(list(getNetworkInterfaces()))
                            .filter(input -> {
                                try {
                                    return input.isUp();
                                } catch (SocketException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                            .transformAndConcat(input -> input.getInterfaceAddresses())
                            .transform(input -> input.getAddress())
                            .filter(input -> !input.isLoopbackAddress())
                            .transform(thisHost -> {
                                for (TransientXListener listenerInConfiguration : listeners) {
                                    Optional<HostAndPort> oHostAndPortInConfiguration = listenerInConfiguration.getHostAndPort();
                                    if (oHostAndPortInConfiguration.isPresent()) {
                                        HostAndPort hostAndPortInConfiguration = oHostAndPortInConfiguration.get();
                                        InetAddress hostAddressInConfiguration = forString(hostAndPortInConfiguration.getHostText());
                                        if (hostAddressInConfiguration.isAnyLocalAddress()) {
                                            return new TransientXListener()
                                                    .setHostAndPort(fromParts(thisHost.getHostAddress(), oHostAndPortInConfiguration.get().getPort()));
                                        } else if (hostAddressInConfiguration.equals(thisHost)) {
                                            return listenerInConfiguration;
                                        }
                                    }
                                }
                                return null;
                            })
                            .filter(notNull());
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

    }
}
