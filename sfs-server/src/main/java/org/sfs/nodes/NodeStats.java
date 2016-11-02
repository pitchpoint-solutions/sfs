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

package org.sfs.nodes;

import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.nodes.GetDocumentsCountForNode;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.rx.ToVoid;
import org.sfs.vo.TransientServiceDef;
import org.sfs.vo.TransientXFileSystem;
import rx.Observable;
import rx.Subscriber;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Runtime.getRuntime;
import static java.nio.file.Files.getFileStore;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static rx.Observable.from;

public class NodeStats {

    private static Logger LOGGER = getLogger(NodeStats.class);
    private VertxContext<Server> vertxContext;
    private boolean started = false;
    private TransientServiceDef transientServiceDef;
    private Long timerId;

    public NodeStats() {
    }

    public Observable<Void> open(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
        return aVoid()
                .flatMap(aVoid -> generate(vertxContext))
                .doOnNext(aVoid -> startTimer())
                .doOnNext(aVoid -> started = true);
    }

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        return aVoid()
                .doOnNext(aVoid -> started = false)
                .doOnNext(aVoid -> stopTimer())
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return aVoid();
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return aVoid();
                })
                .doOnNext(aVoid -> {
                    transientServiceDef = null;
                });
    }

    public Observable<Void> forceUpdate(VertxContext<Server> vertxContext) {
        return generate(vertxContext);
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
                        .subscribe(new Subscriber<Void>() {
                            @Override
                            public void onCompleted() {
                                long refreshInterval = vertxContext.verticle().nodes().getNodeStatsRefreshInterval();
                                timerId = vertxContext.vertx().setTimer(refreshInterval, _this);
                            }

                            @Override
                            public void onError(Throwable e) {
                                LOGGER.debug("Handling Exception", e);
                                long refreshInterval = vertxContext.verticle().nodes().getNodeStatsRefreshInterval();
                                timerId = vertxContext.vertx().setTimer(refreshInterval, _this);
                            }

                            @Override
                            public void onNext(Void aVoid1) {

                            }
                        });
            }
        };
        long refreshInterval = vertxContext.verticle().nodes().getNodeStatsRefreshInterval();
        timerId = vertxContext.vertx().setTimer(refreshInterval, handler);
    }

    protected void stopTimer() {
        if (timerId != null) {
            vertxContext.vertx().cancelTimer(timerId);
        }
    }

    protected void checkStarted() {
        checkState(started, "Not started");
    }


    protected Observable<Void> generate(VertxContext<Server> vertxContext) {
        NodeStats _this = this;
        return aVoid()
                .flatMap(aVoid -> vertxContext.executeBlocking(() -> {
                    try {
                        Nodes nodes = vertxContext.verticle().nodes();
                        List<HostAndPort> publishAddresses = nodes.getPublishAddresses();

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
                                .setPublishAddresses(publishAddresses);

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
                        aVoid()
                                .flatMap(new GetDocumentsCountForNode(vertxContext, nodeId))
                        : just(0L);
    }
}
