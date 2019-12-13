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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsVertx;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.rx.Defer;
import org.sfs.rx.RxHelper;
import org.sfs.rx.Sleep;
import org.sfs.util.HttpClientRequestAndResponse;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;
import rx.exceptions.CompositeException;
import rx.functions.Func1;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.copyOf;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;
import static java.nio.file.Paths.get;
import static org.sfs.rx.Defer.aVoid;

public class Nodes {

    private static final Logger LOGGER = getLogger(Nodes.class);
    private int numberOfObjectReplicas = 0;
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
    private WriteConsistency objectWriteConsistency;

    public Nodes() {
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
            final boolean masterNode,
            WriteConsistency writeConsistency) {

        checkArgument(numberOfObjectReplicas >= 0, "Replicas must be > 0");
        checkArgument(nodeStatsRefreshInterval >= 1000, "RefreshInterval must be greater than 1000");

        this.dataNode = dataNode;
        this.masterNode = masterNode;
        this.numberOfObjectReplicas = numberOfObjectReplicas;
        this.objectWriteConsistency = writeConsistency;
        this.maxPoolSize = maxPoolSize;
        this.connectTimeout = connectTimeout;
        this.responseTimeout = responseTimeout;
        this.nodeIdPath = get(vertxContext.verticle().sfsFileSystem().workingDirectory().toString(), "node", ".nodeId");
        this.volumeManager = new VolumeManager(vertxContext.verticle().sfsFileSystem().workingDirectory());
        this.publishAddresses = copyOf(publicAddresses);
        this.clusterHosts = copyOf(clusterHosts);
        this.nodeStatsRefreshInterval = nodeStatsRefreshInterval;

        return aVoid()
                .flatMap(aVoid -> initNode(vertxContext)
                        .doOnNext(n -> {
                            nodeId = n;
                        }))
                .flatMap(aVoid -> {
                    if (dataNode) {
                        return volumeManager.open(vertxContext);
                    } else {
                        return aVoid();
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

    public int getNumberOfObjectReplicas() {
        return numberOfObjectReplicas;
    }

    public Nodes setNumberOfObjectReplicas(int numberOfObjectReplicas) {
        this.numberOfObjectReplicas = numberOfObjectReplicas;
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

    private Observable<String> initNode(VertxContext<Server> vertxContext) {
        SfsVertx sfsVertx = vertxContext.vertx();
        Context context = sfsVertx.getOrCreateContext();
        return RxHelper.executeBlocking(context, sfsVertx.getBackgroundPool(),
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
                .flatMap(aVoid -> RxHelper.executeBlocking(context, sfsVertx.getBackgroundPool(), () -> {
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

    public WriteConsistency getObjectWriteConsistency() {
        return objectWriteConsistency;
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
        return aVoid()
                .flatMap(aVoid -> {
                    VolumeManager v = volumeManager;
                    volumeManager = null;
                    if (v != null) {
                        return v.close(vertxContext)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Unhandled Exception", throwable);
                                    return aVoid();
                                });
                    }
                    return aVoid();

                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return aVoid();
                })
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling Exception", throwable);
                    return aVoid();
                })
                .doOnNext(aVoid -> {
                    nodeId = null;
                    LOGGER.info("Stopped node " + aNodeId);
                });
    }

    public Observable<HttpClientRequestAndResponse> connectFirstAvailable(Vertx vertx, Collection<HostAndPort> hostAndPorts, Func1<HostAndPort, Observable<HttpClientRequestAndResponse>> supplier) {
        return Observable.defer(() -> {
            Preconditions.checkArgument(!hostAndPorts.isEmpty(), "hostAndPorts cannot be empty");
            AtomicReference<HttpClientRequestAndResponse> ref = new AtomicReference<>();
            List<Throwable> errors = new ArrayList<>();
            return RxHelper.iterate(
                    vertx,
                    hostAndPorts,
                    hostAndPort ->
                            Defer.aVoid()
                                    .flatMap(aVoid -> RxHelper.onErrorResumeNext(50, () -> supplier.call(hostAndPort)))
                                    .doOnNext(httpClientRequestAndResponseAction1 -> Preconditions.checkState(ref.compareAndSet(null, httpClientRequestAndResponseAction1), "Already set"))
                                    .map(httpClientRequestAndResponse -> false)
                                    .onErrorResumeNext(throwable -> {
                                        LOGGER.warn("Handling connect failure to " + hostAndPort, throwable);
                                        errors.add(throwable);
                                        return Defer.just(true);
                                    }))
                    .flatMap(_continue -> {
                        HttpClientRequestAndResponse httpClientRequestAndResponse = ref.get();
                        if (httpClientRequestAndResponse == null) {
                            Preconditions.checkState(!errors.isEmpty(), "Errors cannot be empty");
                            if (errors.size() == 1) {
                                return Observable.error(errors.get(0));
                            } else {
                                return Observable.error(new CompositeException(errors));
                            }
                        } else {
                            return Observable.just(httpClientRequestAndResponse);
                        }
                    });
        });
    }

    public XNode remoteNode(VertxContext<Server> vertxContext, TransientServiceDef serviceDef) {
        return remoteNode(vertxContext, serviceDef.getPublishAddresses());
    }

    public XNode remoteNode(VertxContext<Server> vertxContext, HostAndPort hostAndPort) {
        return remoteNode(vertxContext, Collections.singletonList(hostAndPort));
    }

    public XNode remoteNode(VertxContext<Server> vertxContext, Collection<HostAndPort> hostAndPorts) {
        return new RemoteNode(vertxContext, responseTimeout, hostAndPorts);
    }

    public MasterNode remoteMasterNode(VertxContext<Server> vertxContext, TransientServiceDef serviceDef) {
        return remoteMasterNode(vertxContext, serviceDef.getPublishAddresses());
    }

    public MasterNode remoteMasterNode(VertxContext<Server> vertxContext, HostAndPort hostAndPort) {
        return remoteMasterNode(vertxContext, Collections.singletonList(hostAndPort));
    }

    public MasterNode remoteMasterNode(VertxContext<Server> vertxContext, Collection<HostAndPort> hostAndPorts) {
        return new RemoteMasterNode(vertxContext, responseTimeout, hostAndPorts);
    }
}
