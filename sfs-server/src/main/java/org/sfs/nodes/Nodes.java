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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.vertx.core.http.HttpClient;
import io.vertx.core.logging.Logger;
import org.sfs.HttpClientKey;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.rx.Sleep;
import org.sfs.vo.PersistentServiceDef;
import org.sfs.vo.ServiceDef;
import org.sfs.vo.TransientXListener;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.size;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;
import static java.nio.file.Paths.get;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.UUIDGen.getTimeUUID;
import static rx.Observable.defer;
import static rx.Observable.just;

public class Nodes {

    private static final Logger LOGGER = getLogger(Nodes.class);
    private int numberOfReplicas = 0;
    private int numberOfPrimaries = 1;
    // if true a primary and replica copies of volume data
    // can exist on the same node
    private boolean allowSameNode = false;
    private int maxPoolSize;
    private int connectTimeout;
    private int responseTimeout;
    private VolumeManager volumeManager;
    private boolean dataNode = false;
    private String nodeId;
    private Cache<HttpClientKey, HttpClient> httpClientCache =
            newBuilder()
                    .maximumSize(1000)
                    .expireAfterAccess(30, MINUTES)
                    .removalListener(new RemovalListener<HttpClientKey, HttpClient>() {
                        @Override
                        public void onRemoval(RemovalNotification<HttpClientKey, HttpClient> notification) {
                            if (notification != null) {
                                HttpClient httpClient = notification.getValue();
                                if (httpClient != null) {
                                    httpClient.close();
                                }
                            }
                        }
                    })
                    .build();
    private Path nodeIdPath;
    private boolean masterNode;
    private ClusterInfo clusterInfo = new ClusterInfo();
    private NodeStats nodeStats;
    private ImmutableList<TransientXListener> listeners;
    private final BlockingQueue<Runnable> ioQueue;
    private final BlockingQueue<Runnable> backgroundQueue;

    public Nodes(BlockingQueue<Runnable> ioQueue, BlockingQueue<Runnable> backgroundQueue) {
        this.ioQueue = ioQueue;
        this.backgroundQueue = backgroundQueue;
    }

    public Observable<Void> open(
            VertxContext<Server> vertxContext,
            final Iterable<TransientXListener> listeners,
            final int maxPoolSize,
            final int connectTimeout,
            final int responseTimeout,
            final int numberOfReplicas,
            final long pingInterval,
            final long pingTimeout,
            final boolean dataNode,
            final boolean masterNode) {

        checkArgument(numberOfReplicas >= 0, "Replicas must be >= 0");
        checkArgument(pingInterval >= 1000, "RefreshInterval must be greater than 1000");

        this.dataNode = dataNode;
        this.masterNode = masterNode;
        this.numberOfReplicas = numberOfReplicas;
        this.maxPoolSize = maxPoolSize;
        this.connectTimeout = connectTimeout;
        this.responseTimeout = responseTimeout;
        this.nodeIdPath = get(vertxContext.verticle().sfsFileSystem().workingDirectory().toString(), "node", ".nodeId");
        this.volumeManager = new VolumeManager(vertxContext.verticle().sfsFileSystem().workingDirectory());
        this.listeners = copyOf(listeners);
        this.nodeStats = new NodeStats(this.listeners, pingTimeout, pingInterval);

        return empty()
                .flatMap(aVoid -> initNode(vertxContext)
                        .doOnNext(n -> {
                            nodeId = n;
                        }))
                .flatMap(aVoid -> {
                    if (dataNode) {
                        return volumeManager.open(vertxContext);
                    } else {
                        return empty();
                    }
                })
                .flatMap(aVoid -> nodeStats.open(vertxContext))
                .flatMap(aVoid -> clusterInfo.open(vertxContext))
                .single()
                .doOnNext(aVoid -> {
                    LOGGER.info("Started node " + nodeId);
                });
    }

    public List<TransientXListener> getListeners() {
        return listeners;
    }

    public int getNumberOfPrimaries() {
        return numberOfPrimaries;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public Nodes setNumberOfPrimaries(int numberOfPrimaries) {
        this.numberOfPrimaries = numberOfPrimaries;
        return this;
    }

    public Nodes setNumberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        return this;
    }

    public boolean isAllowSameNode() {
        return allowSameNode;
    }

    public Nodes setAllowSameNode(boolean allowSameNode) {
        this.allowSameNode = allowSameNode;
        return this;
    }

    public boolean isMaster() {
        return masterNode;
    }

    public int getBackgroundQueueSize() {
        return backgroundQueue.size();
    }

    public int getIoQueueSize() {
        return backgroundQueue.size();
    }

    private Observable<String> initNode(VertxContext<Server> vertxContext) {
        return vertxContext.executeBlocking(
                () -> {
                    try {
                        createDirectories(nodeIdPath.getParent());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        createFile(nodeIdPath);
                        // the write will be skipped if the file already exists
                        write(nodeIdPath, getTimeUUID().toString().getBytes(UTF_8));
                    } catch (IOException e) {
                        // do nothing
                    }
                    return (Void) null;
                })
                .flatMap(new Sleep(vertxContext, 1000))
                .flatMap(aVoid -> vertxContext.executeBlocking(() -> {
                    try {
                        return new String(readAllBytes(nodeIdPath), UTF_8);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    @VisibleForTesting
    public HostAndPort getHostAndPort() {
        return listeners.get(0).getHostAndPort().get();
    }

    @VisibleForTesting
    public Observable<Void> forceNodeStatsUpdate(VertxContext<Server> vertxContext) {
        return nodeStats.forceUpdate(vertxContext);
    }

    @VisibleForTesting
    public Observable<Void> forceClusterInfoRefresh(VertxContext<Server> vertxContext) {
        return clusterInfo.forceRefresh(vertxContext);
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getResponseTimeout() {
        return responseTimeout;
    }

    public VolumeManager volumeManager() {
        return volumeManager;
    }

    public boolean isDataNode() {
        return dataNode;
    }

    public String getNodeId() {
        return nodeId;
    }

    public NodeStats getNodeStats() {
        return nodeStats;
    }

    public Observable<Boolean> isOnline(VertxContext<Server> vertxContext) {
        return defer(() -> {
            int expectedNodeCount = getNumberOfPrimaries() + getNumberOfReplicas();
            int count = size(getDataNodes(vertxContext));
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Expected node count was " + expectedNodeCount + ", Active node count was " + count);
            }
            return just(count >= expectedNodeCount);
        });
    }

    public Observable<Optional<PersistentServiceDef>> getMaintainerNode(VertxContext<Server> vertxContext) {
        return just(clusterInfo.getCurrentMaintainerNode());
    }

    public Iterable<XNode<?>> getNodesForVolume(VertxContext<Server> vertxContext, String volumeId) {
        List<PersistentServiceDef> nodes = clusterInfo.getNodesForVolume(volumeId);
        return from(nodes)
                .transformAndConcat(input -> candidateConnections(vertxContext, input));
    }

    public Iterable<PersistentServiceDef> getDataNodes(VertxContext<Server> vertxContext) {
        return clusterInfo.getDataNodes();
    }

    public Iterable<PersistentServiceDef> getNodes() {
        return clusterInfo.getNodes();
    }

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        String aNodeId = nodeId;
        return empty()
                .doOnNext(aVoid -> {
                    httpClientCache.invalidateAll();
                })
                .flatMap(aVoid -> {
                    VolumeManager v = volumeManager;
                    volumeManager = null;
                    if (v != null) {
                        return v.close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return empty();
                                });
                    }
                    return empty();

                })
                .flatMap(aVoid -> {
                    if (clusterInfo != null) {
                        return clusterInfo.close(vertxContext);
                    } else {
                        return just(null);
                    }
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return empty();
                })
                .flatMap(aVoid -> {
                    if (nodeStats != null) {
                        return nodeStats.close(vertxContext);
                    } else {
                        return just(null);
                    }
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return empty();
                })
                .doOnNext(aVoid -> {
                    nodeId = null;
                    LOGGER.info("Stopped node " + aNodeId);
                });
    }

    public Iterable<XNode<? extends XNode>> candidateConnections(final VertxContext<Server> vertxContext, final ServiceDef<? extends ServiceDef> serviceDef) {
        String nodeId = serviceDef.getId();
        if (this.nodeId.equals(nodeId)) {
            if (volumeManager == null) {
                return emptyList();
            } else {
                return singletonList(new LocalNode(vertxContext, volumeManager));
            }
        } else {
            return from(serviceDef.getListeners())
                    .filter(input -> input.getHostAndPort().isPresent())
                    .transform(input -> new RemoteNode(vertxContext, nodeId, responseTimeout, input.getHostAndPort().get()));
        }
    }

}
