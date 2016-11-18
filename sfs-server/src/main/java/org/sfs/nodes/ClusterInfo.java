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
import com.google.common.base.Preconditions;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.Defer;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.vo.TransientServiceDef;
import org.sfs.vo.XVolume;
import rx.Observable;
import rx.Subscriber;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.FluentIterable.from;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.filesystem.volume.Volume.Status;
import static org.sfs.rx.Defer.aVoid;

public class ClusterInfo {

    private static final Logger LOGGER = getLogger(ClusterInfo.class);
    private final long refreshInterval = SECONDS.toMillis(1);
    private boolean started = false;
    private VertxContext<Server> vertxContext;
    private volatile List<TransientServiceDef> allNodes;
    private volatile Map<String, TransientServiceDef> nodesByStartedVolume;
    private volatile NavigableMap<Long, Set<String>> startedVolumeIdByUseableSpace;
    private volatile int numberOfStartedVolumes;
    private volatile TransientServiceDef currentMaintainerNode;
    private volatile List<TransientServiceDef> masterNodes;
    private Long timerId;

    public Observable<Void> open(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
        return aVoid()
                .flatMap(aVoid -> updateClusterInfo(vertxContext))
                .doOnNext(aVoid -> startTimer())
                .doOnNext(aVoid -> started = true);
    }

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        return aVoid()
                .doOnNext(aVoid -> started = false)
                .doOnNext(aVoid -> stopTimer())
                .doOnNext(aVoid -> {
                    if (nodesByStartedVolume != null) {
                        nodesByStartedVolume.clear();
                        nodesByStartedVolume = null;
                    }
                    currentMaintainerNode = null;
                    masterNodes = null;
                });
    }

    public List<TransientServiceDef> getAllNodes() {
        return allNodes;
    }

    public long getRefreshInterval() {
        return refreshInterval;
    }

    public int getNumberOfStartedVolumes() {
        return numberOfStartedVolumes;
    }

    public NavigableMap<Long, Set<String>> getStartedVolumeIdByUseableSpace() {
        NavigableMap<Long, Set<String>> snapshot = startedVolumeIdByUseableSpace;
        return snapshot != null ? snapshot : Collections.emptyNavigableMap();
    }

    public Observable<Void> forceRefresh(VertxContext<Server> vertxContext) {
        return aVoid()
                .doOnNext(aVoid -> checkStarted())
                .flatMap(aVoid -> updateClusterInfo(vertxContext));
    }

    public Iterable<TransientServiceDef> getNodesWithStartedVolumes() {
        checkStarted();
        Map<String, TransientServiceDef> snapshot = nodesByStartedVolume;
        if (snapshot == null) {
            return emptyList();
        }
        return snapshot.values();
    }

    public Observable<Boolean> isOnline() {
        return Observable.defer(() -> {
            try {
                return Defer.just(getCurrentMasterNode() != null);
            } catch (Throwable e) {
                return Defer.just(false);
            }
        });
    }

    public Optional<XNode> getNodeForVolume(VertxContext<Server> vertxContext, String volumeId) {
        Nodes nodes = vertxContext.verticle().nodes();
        TransientServiceDef serviceDef = nodesByStartedVolume.get(volumeId);
        if (serviceDef != null) {
            return Optional.of(nodes.remoteNode(vertxContext, serviceDef));
        } else {
            return Optional.absent();
        }
    }

    public Optional<TransientServiceDef> getServiceDefForVolume(String volumeId) {
        checkStarted();
        Map<String, TransientServiceDef> snapshot = nodesByStartedVolume;
        return snapshot != null ? Optional.fromNullable(snapshot.get(volumeId)) : Optional.absent();
    }

    public Optional<TransientServiceDef> getCurrentMaintainerNode() {
        checkStarted();
        return fromNullable(currentMaintainerNode);
    }

    public TransientServiceDef getCurrentMasterNode() {
        checkStarted();
        List<TransientServiceDef> snapshot = masterNodes;
        Preconditions.checkState(!snapshot.isEmpty(), "no elected master node");
        Preconditions.checkState(snapshot.size() <= 1, "more than one elected master node");
        return snapshot.get(0);
    }

    public Iterable<TransientServiceDef> getDataNodes() {
        checkStarted();
        return from(getNodesWithStartedVolumes())
                .filter(input -> {
                    Boolean dataNode = input.getDataNode().orNull();
                    return TRUE.equals(dataNode);
                });
    }

    protected void startTimer() {
        Handler<Long> handler = new Handler<Long>() {

            Handler<Long> _this = this;

            @Override
            public void handle(Long event) {
                updateClusterInfo(vertxContext)
                        .subscribe(new Subscriber<Void>() {
                            @Override
                            public void onCompleted() {
                                timerId = vertxContext.vertx().setTimer(refreshInterval, _this);
                            }

                            @Override
                            public void onError(Throwable e) {
                                LOGGER.debug("Handling Exception", e);
                                timerId = vertxContext.vertx().setTimer(refreshInterval, _this);
                            }

                            @Override
                            public void onNext(Void aVoid1) {

                            }
                        });
            }
        };
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

    protected Observable<Void> updateClusterInfo(VertxContext<Server> vertxContext) {
        Nodes nodes = vertxContext.verticle().nodes();
        List<TransientServiceDef> transientServiceDefs = new ArrayList<>();
        return RxHelper.iterate(vertxContext.vertx(), nodes.getClusterHosts(), hostAndPort -> {
            XNode xNode = nodes.remoteNode(vertxContext, hostAndPort);
            return xNode.getNodeStats()
                    .doOnNext(transientServiceDefOptional -> {
                        if (transientServiceDefOptional.isPresent()) {
                            transientServiceDefs.add(transientServiceDefOptional.get());
                        }
                    })
                    .map(transientServiceDefOptional -> true)
                    .onErrorResumeNext(throwable -> {
                        LOGGER.warn("Handling Connect Error", throwable);
                        return Defer.just(true);
                    });
        })
                .map(new ToVoid<>())
                .doOnNext(aVoid -> {

                    int updatedNumberOfStartedVolumes = 0;

                    Map<String, TransientServiceDef> updatedNodesByStartedVolume = new HashMap<>();
                    NavigableMap<Long, Set<String>> updatedStartedVolumeIdByUseableSpace = new TreeMap<>();
                    List<TransientServiceDef> updatedMasterNodes = new ArrayList<>();

                    TransientServiceDef candidateMaintainerNode = null;

                    for (TransientServiceDef transientServiceDef : transientServiceDefs) {

                        if (Boolean.TRUE.equals(transientServiceDef.getMaster().orNull())) {
                            updatedMasterNodes.add(transientServiceDef);
                        }

                        if (candidateMaintainerNode == null) {
                            candidateMaintainerNode = transientServiceDef;
                        } else {
                            long currentDocumentCount = candidateMaintainerNode.getDocumentCount().or(0L);
                            long candidateDocumentCount = transientServiceDef.getDocumentCount().or(0L);
                            if (candidateDocumentCount < currentDocumentCount) {
                                candidateMaintainerNode = transientServiceDef;
                            }
                        }

                        for (XVolume<?> xVolume : transientServiceDef.getVolumes()) {
                            Optional<String> oVolumeId = xVolume.getId();
                            Optional<Status> oStatus = xVolume.getStatus();
                            Optional<Long> oUseableSpace = xVolume.getUsableSpace();
                            if (oVolumeId.isPresent()
                                    && oStatus.isPresent()
                                    && oUseableSpace.isPresent()) {
                                Status status = oStatus.get();
                                if (Status.STARTED.equals(status)) {
                                    String volumeId = oVolumeId.get();

                                    long useableSpace = oUseableSpace.get();

                                    updatedNumberOfStartedVolumes++;
                                    Set<String> volumeIdsForSpace = updatedStartedVolumeIdByUseableSpace.get(useableSpace);
                                    if (volumeIdsForSpace == null) {
                                        volumeIdsForSpace = new HashSet<>();
                                        updatedStartedVolumeIdByUseableSpace.put(useableSpace, volumeIdsForSpace);
                                    }
                                    volumeIdsForSpace.add(volumeId);


                                    updatedNodesByStartedVolume.put(volumeId, transientServiceDef);
                                }
                            }
                        }
                    }

                    numberOfStartedVolumes = updatedNumberOfStartedVolumes;
                    startedVolumeIdByUseableSpace = updatedStartedVolumeIdByUseableSpace;
                    nodesByStartedVolume = updatedNodesByStartedVolume;
                    currentMaintainerNode = candidateMaintainerNode;
                    allNodes = transientServiceDefs;
                    masterNodes = updatedMasterNodes;
                });
    }
}
