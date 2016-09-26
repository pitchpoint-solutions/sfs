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
import com.google.common.collect.ListMultimap;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.nodes.GetServiceDefs;
import org.sfs.rx.ToVoid;
import org.sfs.vo.PersistentServiceDef;
import org.sfs.vo.XVolume;
import rx.Observable;
import rx.Subscriber;

import java.util.List;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ArrayListMultimap.create;
import static com.google.common.collect.FluentIterable.from;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.filesystem.volume.Volume.Status;
import static org.sfs.filesystem.volume.Volume.Status.STARTED;
import static org.sfs.rx.Defer.empty;

public class ClusterInfo {

    private static final Logger LOGGER = getLogger(ClusterInfo.class);
    private final long refreshInterval = SECONDS.toMillis(1);
    private boolean started = false;
    private VertxContext<Server> vertxContext;
    private ListMultimap<String, PersistentServiceDef> nodesByVolume;
    private PersistentServiceDef maintainerNode;
    private Long timerId;

    public Observable<Void> open(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
        return empty()
                .flatMap(aVoid -> updateList(vertxContext))
                .doOnNext(aVoid -> startTimer())
                .doOnNext(aVoid -> started = true);
    }

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        return empty()
                .doOnNext(aVoid -> started = false)
                .doOnNext(aVoid -> stopTimer())
                .doOnNext(aVoid -> {
                    if (nodesByVolume != null) {
                        nodesByVolume.clear();
                        nodesByVolume = null;
                    }
                    maintainerNode = null;
                });
    }

    public Observable<Void> forceRefresh(VertxContext<Server> vertxContext) {
        return empty()
                .doOnNext(aVoid -> checkStarted())
                .flatMap(aVoid -> updateList(vertxContext));
    }

    public Iterable<PersistentServiceDef> getNodes() {
        checkStarted();
        if (nodesByVolume == null) {
            return emptyList();
        }
        return nodesByVolume.values();
    }

    public List<PersistentServiceDef> getNodesForVolume(String volumeId) {
        checkStarted();
        return nodesByVolume != null ? nodesByVolume.get(volumeId) : emptyList();
    }

    public Optional<PersistentServiceDef> getCurrentMaintainerNode() {
        checkStarted();
        return fromNullable(maintainerNode);
    }

    public Iterable<PersistentServiceDef> getMasterNodes() {
        checkStarted();
        return from(getNodes())
                .filter(input -> {
                    Boolean masterNode = input.getMaster().orNull();
                    return TRUE.equals(masterNode);
                });
    }

    public Iterable<PersistentServiceDef> getDataNodes() {
        checkStarted();
        return from(getNodes())
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
                updateList(vertxContext)
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

    protected Observable<Void> updateList(VertxContext<Server> vertxContext) {
        return empty()
                .flatMap(new GetServiceDefs(vertxContext))
                .toList()
                .doOnNext(persistentServiceDefs -> {

                    ListMultimap<String, PersistentServiceDef> map = create();

                    PersistentServiceDef candidateMaintainerNode = null;

                    for (PersistentServiceDef persistentServiceDef : persistentServiceDefs) {

                        if (candidateMaintainerNode == null) {
                            candidateMaintainerNode = persistentServiceDef;
                        } else {
                            long currentDocumentCount = candidateMaintainerNode.getDocumentCount().or(0L);
                            long candidateDocumentCount = persistentServiceDef.getDocumentCount().or(0L);
                            if (candidateDocumentCount < currentDocumentCount) {
                                candidateMaintainerNode = persistentServiceDef;
                            }
                        }

                        for (XVolume<? extends XVolume> xVolume : persistentServiceDef.getVolumes()) {
                            Optional<Status> oStatus = xVolume.getStatus();
                            if (oStatus.isPresent()) {
                                if (STARTED.equals(oStatus.get())) {
                                    map.put(xVolume.getId().get(), persistentServiceDef);
                                }
                            }
                        }
                    }

                    nodesByVolume = map;
                    maintainerNode = candidateMaintainerNode;
                })
                .map(new ToVoid<>());
    }
}
