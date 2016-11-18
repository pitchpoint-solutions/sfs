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
import com.google.common.collect.Iterables;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.MultiEndableWriteStream;
import org.sfs.io.PipedEndableWriteStream;
import org.sfs.io.PipedReadStream;
import org.sfs.rx.Defer;
import org.sfs.rx.RxHelper;
import org.sfs.util.MessageDigestFactory;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import static com.google.common.collect.Iterables.toArray;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.util.Collections.singletonList;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;

public class VolumeReplicaGroup {

    private static final Logger LOGGER = getLogger(VolumeReplicaGroup.class);
    private final VertxContext<Server> vertxContext;
    private int numberOfObjectCopies;
    private boolean allowSameNode = false;
    private Set<String> excludeVolumes;
    private ClusterInfo clusterInfo;

    public VolumeReplicaGroup(VertxContext<Server> vertxContext, int numberOfObjectCopies) {
        this.vertxContext = vertxContext;
        this.numberOfObjectCopies = numberOfObjectCopies;
        this.clusterInfo = vertxContext.verticle().getClusterInfo();
    }

    public boolean isAllowSameNode() {
        return allowSameNode;
    }

    public int getNumberOfObjectCopies() {
        return numberOfObjectCopies;
    }

    public VolumeReplicaGroup setNumberOfObjectCopies(int numberOfObjectCopies) {
        this.numberOfObjectCopies = numberOfObjectCopies;
        return this;
    }

    public VolumeReplicaGroup setAllowSameNode(boolean allowSameNode) {
        this.allowSameNode = allowSameNode;
        return this;
    }

    public VolumeReplicaGroup setExcludeVolumeIds(Iterable<String> volumeIds) {
        if (excludeVolumes != null) {
            excludeVolumes.clear();
        } else {
            excludeVolumes = new HashSet<>();
        }
        Iterables.addAll(excludeVolumes, volumeIds);
        return this;
    }

    public int getQuorumNumber() {
        return (numberOfObjectCopies / 2) + 1;
    }

    public int getQuorumMinNumberOfCopies() {
        return numberOfObjectCopies > 0 ? Math.max(getQuorumNumber(), 1) : 1;
    }

    public Observable<List<DigestBlob>> consume(final long length, final MessageDigestFactory messageDigestFactories, ReadStream<Buffer> src) {
        return consume(length, singletonList(messageDigestFactories), src);
    }

    public Observable<List<DigestBlob>> consume(final long length, final Iterable<MessageDigestFactory> messageDigestFactories, ReadStream<Buffer> src) {
        return calculateNodeWriteStreamBlobs(length, toArray(messageDigestFactories, MessageDigestFactory.class))
                .flatMap(nodeWriteStreamBlobs -> {
                    int size = nodeWriteStreamBlobs.size();
                    List<Observable<DigestBlob>> oDigests = new ArrayList<>(size);
                    List<BufferEndableWriteStream> writeStreams = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        PipedReadStream readStream = new PipedReadStream();
                        PipedEndableWriteStream writeStream = new PipedEndableWriteStream(readStream);
                        Observable<DigestBlob> oDigest = nodeWriteStreamBlobs.get(i).consume(readStream);
                        oDigests.add(oDigest);
                        writeStreams.add(writeStream);
                    }

                    MultiEndableWriteStream multiWriteStream = new MultiEndableWriteStream(writeStreams);
                    Observable<Void> producer = pump(src, multiWriteStream).single();
                    Observable<List<DigestBlob>> consumer =
                            Observable.mergeDelayError(oDigests)
                                    .toList()
                                    .single();
                    // the zip operator will not work here
                    // since the subscriptions need to run
                    // in parallel due to the pipe connections
                    return combineSinglesDelayError(
                            producer,
                            consumer,
                            (aVoid, response) -> response);
                });
    }

    public Observable<List<ConnectedVolume>> getReplicaVolumesForWrite(List<ConnectedVolume> toIgnore, long requiredSpace, int numberOfReplicas, boolean allowSameNode, MessageDigestFactory... messageDigestFactories) {
        if (numberOfReplicas > 0) {
            return getVolumesForWrite(clusterInfo.getStartedVolumeIdByUseableSpace(), toIgnore, requiredSpace, numberOfReplicas, allowSameNode, messageDigestFactories);
        }
        return Defer.just(Collections.emptyList());
    }

    protected Observable<List<NodeWriteStreamBlob>> calculateNodeWriteStreamBlobs(final long length, final MessageDigestFactory... messageDigestFactories) {
        int replicaQuorumNumber = getQuorumMinNumberOfCopies();
        return getReplicaVolumesForWrite(Collections.emptyList(), length, numberOfObjectCopies, allowSameNode, messageDigestFactories)
                .doOnNext(targetReplicaVolumes -> checkFoundSufficientVolumes(targetReplicaVolumes.size(), replicaQuorumNumber, false))
                .flatMap(Observable::from)
                .map(ConnectedVolume::getNodeWriteStreamBlob)
                .toList();

    }

    protected void checkFoundSufficientVolumes(int actualMatches, int expectedMatches, boolean primary) {
        if (actualMatches < expectedMatches) {
            throw new InsufficientReplicaVolumesAvailableException(expectedMatches, actualMatches);
        }
    }

    protected Observable<List<ConnectedVolume>> getVolumesForWrite(NavigableMap<Long, Set<String>> volumesBySpace, List<ConnectedVolume> toSkip, long requiredSpace, int numberToCollect, boolean allowSameNode, MessageDigestFactory... messageDigestFactories) {
        if (volumesBySpace != null && numberToCollect > 0) {

            NavigableMap<Long, Set<String>> descendingMap = volumesBySpace.descendingMap();

            List<ConnectedVolume> results = new ArrayList<>(numberToCollect);

            Set<String> seenNodes = new HashSet<>();
            Set<String> seenVolumes = new HashSet<>();
            for (ConnectedVolume primaryTargetVolume : toSkip) {
                seenNodes.add(primaryTargetVolume.getNodeId());
                seenVolumes.add(primaryTargetVolume.getVolumeId());
            }
            if (excludeVolumes != null) {
                seenVolumes.addAll(excludeVolumes);
            }
            Vertx vertx = vertxContext.vertx();
            return RxHelper.iterate(vertx, descendingMap.entrySet(), entry -> {
                long useableSpace = entry.getKey();
                if (useableSpace * 0.90 >= requiredSpace) {
                    Set<String> volumeIds = entry.getValue();
                    return RxHelper.iterate(vertx, volumeIds, volumeId -> {
                        if (seenVolumes.add(volumeId)) {
                            Optional<TransientServiceDef> oServiceDef = clusterInfo.getServiceDefForVolume(volumeId);
                            if (oServiceDef.isPresent()) {
                                TransientServiceDef serviceDef = oServiceDef.get();
                                Optional<XNode> oXNode = clusterInfo.getNodeForVolume(vertxContext, volumeId);
                                if (oXNode.isPresent()) {
                                    XNode xNode = oXNode.get();
                                    return xNode.createWriteStream(volumeId, requiredSpace, messageDigestFactories)
                                            .onErrorResumeNext(throwable -> {
                                                LOGGER.warn(String.format("Failed to connect to volume %s", volumeId), throwable);
                                                return Defer.just(null);
                                            })
                                            .doOnNext(nodeWriteStreamBlob -> {
                                                if (nodeWriteStreamBlob != null) {
                                                    ConnectedVolume connectedVolume = new ConnectedVolume();
                                                    connectedVolume.setxNode(xNode);
                                                    connectedVolume.setVolumeId(volumeId);
                                                    connectedVolume.setNodeId(serviceDef.getId());
                                                    connectedVolume.setNodeWriteStreamBlob(nodeWriteStreamBlob);
                                                    if (allowSameNode || seenNodes.add(connectedVolume.getNodeId())) {
                                                        results.add(connectedVolume);
                                                    }
                                                }
                                            })
                                            .map(nodeWriteStreamBlob -> {
                                                if (results.size() >= numberToCollect) {
                                                    return false;
                                                } else {
                                                    return true;
                                                }
                                            });
                                } else {
                                    return Defer.just(true);
                                }
                            }
                        }
                        return Defer.just(true);
                    });
                } else {
                    return Defer.just(true);
                }
            }).map(aBoolean -> results);
        }
        return Defer.just(Collections.emptyList());
    }

    public static class ConnectedVolume {

        private String nodeId;
        private XNode xNode;
        private String volumeId;
        private NodeWriteStreamBlob nodeWriteStreamBlob;

        public ConnectedVolume(String nodeId, XNode xNode, String volumeId) {
            this.nodeId = nodeId;
            this.xNode = xNode;
            this.volumeId = volumeId;
        }

        public ConnectedVolume() {
        }

        public NodeWriteStreamBlob getNodeWriteStreamBlob() {
            return nodeWriteStreamBlob;
        }

        public ConnectedVolume setNodeWriteStreamBlob(NodeWriteStreamBlob nodeWriteStreamBlob) {
            this.nodeWriteStreamBlob = nodeWriteStreamBlob;
            return this;
        }

        public void setNodeId(String nodeId) {
            this.nodeId = nodeId;
        }

        public void setxNode(XNode xNode) {
            this.xNode = xNode;
        }

        public void setVolumeId(String volumeId) {
            this.volumeId = volumeId;
        }

        public String getNodeId() {
            return nodeId;
        }

        public XNode getxNode() {
            return xNode;
        }

        public String getVolumeId() {
            return volumeId;
        }
    }

}
