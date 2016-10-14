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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.rx.Sleep;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.ImmutableList.copyOf;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;
import static java.nio.file.Paths.get;
import static java.util.Collections.singletonList;
import static org.sfs.rx.Defer.empty;

public class Nodes {

    private static final Logger LOGGER = getLogger(Nodes.class);
    private int numberOfObjectReplicasReplicas = 1;
    // if true a primary and replica copies of volume data
    // can exist on the same node
    private boolean allowSameNode = false;
    private int maxPoolSize;
    private int connectTimeout;
    private int responseTimeout;
    private VolumeManager volumeManager;
    private boolean dataNode = false;
    private String nodeId;
    private Path nodeIdPath;
    private boolean masterNode;
    private long nodeStatsRefreshInterval;
    private ImmutableList<HostAndPort> clusterHosts;
    private ImmutableList<HostAndPort> publishAddresses;
    private final BlockingQueue<Runnable> ioQueue;
    private final BlockingQueue<Runnable> backgroundQueue;

    public Nodes(BlockingQueue<Runnable> ioQueue, BlockingQueue<Runnable> backgroundQueue) {
        this.ioQueue = ioQueue;
        this.backgroundQueue = backgroundQueue;
    }

    public Observable<Void> open(
            VertxContext<Server> vertxContext,
            final Iterable<HostAndPort> publicAddresses,
            final Iterable<HostAndPort> clusterHosts,
            final int maxPoolSize,
            final int connectTimeout,
            final int responseTimeout,
            final int numberOfObjectReplicas,
            final long nodeStatsRefreshInterval,
            final boolean dataNode,
            final boolean masterNode) {

        checkArgument(numberOfObjectReplicas > 0, "Replicas must be > 0");
        checkArgument(nodeStatsRefreshInterval >= 1000, "RefreshInterval must be greater than 1000");

        this.dataNode = dataNode;
        this.masterNode = masterNode;
        this.numberOfObjectReplicasReplicas = numberOfObjectReplicas;
        this.maxPoolSize = maxPoolSize;
        this.connectTimeout = connectTimeout;
        this.responseTimeout = responseTimeout;
        this.nodeIdPath = get(vertxContext.verticle().sfsFileSystem().workingDirectory().toString(), "node", ".nodeId");
        this.volumeManager = new VolumeManager(vertxContext.verticle().sfsFileSystem().workingDirectory());
        this.publishAddresses = copyOf(publicAddresses);
        this.clusterHosts = copyOf(clusterHosts);
        this.nodeStatsRefreshInterval = nodeStatsRefreshInterval;

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
                .single()
                .doOnNext(aVoid -> {
                    LOGGER.info("Started node " + nodeId);
                });
    }

    public long getNodeStatsRefreshInterval() {
        return nodeStatsRefreshInterval;
    }

    public List<HostAndPort> getPublishAddresses() {
        return publishAddresses;
    }

    public ImmutableList<HostAndPort> getClusterHosts() {
        return clusterHosts;
    }

    public int getNumberOfObjectReplicasReplicas() {
        return numberOfObjectReplicasReplicas;
    }

    public Nodes setNumberOfObjectReplicasReplicas(int numberOfObjectReplicasReplicas) {
        this.numberOfObjectReplicasReplicas = numberOfObjectReplicasReplicas;
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
                        write(nodeIdPath, UUID.randomUUID().toString().getBytes(UTF_8));
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
        return publishAddresses.get(0);
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

    public Observable<Void> close(VertxContext<Server> vertxContext) {
        String aNodeId = nodeId;
        return empty()
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
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return empty();
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

    public Iterable<XNode> createNodes(final VertxContext<Server> vertxContext, TransientServiceDef serviceDef) {
        boolean localNode = false;
        for (HostAndPort targetNodeAddress : serviceDef.getPublishAddresses()) {
            if (publishAddresses.contains(targetNodeAddress)) {
                localNode = true;
                break;
            }
        }
        if (localNode) {
            return singletonList(new LocalNode(vertxContext, volumeManager));
        } else {
            return from(serviceDef.getPublishAddresses())
                    .transform(input -> new RemoteNode(vertxContext, responseTimeout, input));
        }
    }

    public XNode createNode(final VertxContext<Server> vertxContext, HostAndPort hostAndPort) {
        if (publishAddresses.contains(hostAndPort)) {
            return new LocalNode(vertxContext, volumeManager);
        } else {
            return new RemoteNode(vertxContext, responseTimeout, hostAndPort);
        }
    }

}
