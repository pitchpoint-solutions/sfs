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
import com.google.common.net.HostAndPort;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.io.MultiEndableWriteStream;
import org.sfs.io.PipedEndableWriteStream;
import org.sfs.io.PipedReadStream;
import org.sfs.rx.Holder2;
import org.sfs.util.MessageDigestFactory;
import org.sfs.vo.PersistentServiceDef;
import org.sfs.vo.XVolume;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Joiner.on;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.lang.Long.compare;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.sort;
import static java.util.Objects.hash;
import static org.sfs.filesystem.volume.Volume.Status.STARTED;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.nodes.ReplicaGroup.Key.nodeIdentity;
import static org.sfs.nodes.ReplicaGroup.Key.volumeIdentity;
import static org.sfs.rx.Defer.just;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.rx.RxHelper.iterate;
import static rx.Observable.defer;
import static rx.Observable.from;

public class ReplicaGroup {

    private static final Logger LOGGER = getLogger(ReplicaGroup.class);
    private final VertxContext<Server> vertxContext;
    private int numberOfPrimaries;
    private int numberOfReplicas;
    private boolean allowSameNode = false;

    public ReplicaGroup(VertxContext<Server> vertxContext, int numberOfPrimaries, int numberOfReplicas, boolean allowSameNode) {
        this.vertxContext = vertxContext;
        this.numberOfPrimaries = numberOfPrimaries;
        this.numberOfReplicas = numberOfReplicas;
        this.allowSameNode = allowSameNode;
    }

    public boolean isAllowSameNode() {
        return allowSameNode;
    }

    public int getNumberOfPrimaries() {
        return numberOfPrimaries;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public ReplicaGroup setNumberOfPrimaries(int numberOfPrimaries) {
        this.numberOfPrimaries = numberOfPrimaries;
        return this;
    }

    public ReplicaGroup setNumberOfReplicas(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
        return this;
    }

    public ReplicaGroup setAllowSameNode(boolean allowSameNode) {
        this.allowSameNode = allowSameNode;
        return this;
    }

    protected long sumUseableSpace(List<XVolume<? extends XVolume>> volumes) {
        long sum = 0;
        for (XVolume<? extends XVolume> volume : volumes) {
            Optional<Long> usableSpace = volume.getUsableSpace();
            if (usableSpace.isPresent()) {
                long space = usableSpace.get();
                if (space > 0) {
                    long newSum = space + sum;
                    if (newSum < sum) {
                        sum = MAX_VALUE;
                        break;
                    } else {
                        sum = newSum;
                    }
                }
            }
        }
        return sum;
    }

    public Observable<List<Holder2<XNode<? extends XNode>, DigestBlob>>> consume(Iterable<PersistentServiceDef> oPersistentServiceDefs, final long length, final MessageDigestFactory messageDigestFactories, ReadStream<Buffer> src) {
        return consume(oPersistentServiceDefs, length, singletonList(messageDigestFactories), src);
    }

    public Observable<List<Holder2<XNode<? extends XNode>, DigestBlob>>> consume(Iterable<PersistentServiceDef> oPersistentServiceDefs, final long length, final Iterable<MessageDigestFactory> messageDigestFactories, ReadStream<Buffer> src) {
        return calculateNodeWriteStreamBlobs(oPersistentServiceDefs, length, toArray(messageDigestFactories, MessageDigestFactory.class))
                .flatMap(Observable::from)
                .map(nodeWriteStreamBlob -> new Holder2<>(nodeWriteStreamBlob, new PipedEndableWriteStream(new PipedReadStream())))
                .toList()
                .flatMap(holders -> {

                    Iterable<PipedEndableWriteStream> i =
                            FluentIterable.from(holders)
                                    .transform(input -> input.value1());

                    // copy data from the src read stream to the queue of the read stream
                    // that is going to clone the buffers to the destination queue of
                    // each read stream that is going to be consumed by the volume on the node
                    MultiEndableWriteStream multiWriteStream = new MultiEndableWriteStream(toArray(i, PipedEndableWriteStream.class));
                    Observable<Void> producer = pump(src, multiWriteStream).single();
                    Observable<List<Holder2<XNode<? extends XNode>, DigestBlob>>> consumer =
                            from(holders)
                                    .flatMap(holder ->
                                            holder.value0().consume(holder.value1().readStream())
                                                    .map(digestBlob -> new Holder2<XNode<? extends XNode>, DigestBlob>(holder.value0().getNode(), digestBlob)))
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

    protected Observable<List<NodeWriteStreamBlob>> calculateNodeWriteStreamBlobs(Iterable<PersistentServiceDef> oPersistentServiceDefs, final long length, final MessageDigestFactory... messageDigestFactories) {
        return defer(() -> {
            Nodes nodes = vertxContext.verticle().nodes();
            return from(oPersistentServiceDefs)
                    .map(PersistentServiceDef::copy)
                    .toSortedList((lft, rgt) -> {
                        long sum1 = sumUseableSpace(lft.getVolumes());
                        long sum2 = sumUseableSpace(rgt.getVolumes());
                        return compare(sum2, sum1); // service defs with the most free space first;
                    })
                    .map(persistentServiceDefs -> {
                        for (PersistentServiceDef persistentServiceDef : persistentServiceDefs) {
                            sort(persistentServiceDef.getVolumes(), (o1, o2) -> {
                                Optional<Long> oSpace1 = o1.getUsableSpace();
                                Optional<Long> oSpace2 = o2.getUsableSpace();
                                long space1 = oSpace1.isPresent() ? oSpace1.get() : MIN_VALUE;
                                long space2 = oSpace2.isPresent() ? oSpace2.get() : MIN_VALUE;
                                return compare(space2, space1); // volumes with the most free space first;
                            });
                        }
                        return persistentServiceDefs;
                    })
                    .flatMap(persistentServiceDefs ->
                            collectWriteStreams(
                                    emptyMap(),
                                    numberOfPrimaries,
                                    true,
                                    nodes,
                                    persistentServiceDefs,
                                    length,
                                    messageDigestFactories)
                                    .map(primaryWriteStreams -> {
                                        if (LOGGER.isDebugEnabled()) {
                                            LOGGER.debug("Primary nodes are " + join(primaryWriteStreams));
                                        }
                                        checkFoundSufficientVolumes(persistentServiceDefs, primaryWriteStreams, numberOfPrimaries, true);
                                        return primaryWriteStreams;
                                    })
                                    .flatMap(primaryWriteStreams ->
                                            collectWriteStreams(primaryWriteStreams, numberOfReplicas, false, nodes, persistentServiceDefs, length, messageDigestFactories)
                                                    .map(replicaWriteStreams -> {
                                                        if (LOGGER.isDebugEnabled()) {
                                                            LOGGER.debug("Replica nodes are " + join(replicaWriteStreams));
                                                        }
                                                        checkFoundSufficientVolumes(persistentServiceDefs, replicaWriteStreams, numberOfReplicas, false);
                                                        return replicaWriteStreams;
                                                    })
                                                    .map(replicaWriteStreams -> newArrayList(concat(primaryWriteStreams.values(), replicaWriteStreams.values())))));
        });

    }

    protected void checkFoundSufficientVolumes(List<PersistentServiceDef> persistentServiceDefs, Map<Key, NodeWriteStreamBlob> matches, int expectedMatches, boolean primary) {
        if (matches.size() < expectedMatches) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Available volumes were " +
                        FluentIterable.from(persistentServiceDefs)
                                .transform(input -> input.toJsonObject().encodePrettily()));
            }
            if (primary) {
                throw new InsufficientPrimaryVolumeAvailableException(expectedMatches, matches.size());
            } else {
                throw new InsufficientReplicaVolumesAvailableException(expectedMatches, matches.size());
            }
        }
    }

    protected Observable<Map<Key, NodeWriteStreamBlob>> collectWriteStreams(
            Map<Key, NodeWriteStreamBlob> exitingWriteStreamsByNode,
            int numberToCollect,
            boolean primary,
            Nodes nodes,
            List<PersistentServiceDef> persistentServiceDefs,
            long length,
            MessageDigestFactory[] messageDigestFactories) {
        return defer(() -> {
            if (numberToCollect <= 0) {
                return just(emptyMap());
            } else {
                Map<Key, NodeWriteStreamBlob> writeStreamsByNode = new HashMap<>(numberToCollect);

                return iterate(
                        persistentServiceDefs,
                        persistentServiceDef ->
                                iterate(
                                        persistentServiceDef.getVolumes(),
                                        xVolume -> {

                                            Key key = allowSameNode ? volumeIdentity(persistentServiceDef, xVolume) : nodeIdentity(persistentServiceDef);

                                            if (LOGGER.isDebugEnabled()) {
                                                LOGGER.debug("Maybe connecting to volume " + xVolume.toJsonObject().encodePrettily());
                                            }

                                            if (writeStreamsByNode.size() < numberToCollect
                                                    && !writeStreamsByNode.containsKey(key)
                                                    && !exitingWriteStreamsByNode.containsKey(key)
                                                    && STARTED.equals(xVolume.getStatus().get())
                                                    && (primary ? xVolume.isPrimary().get() : xVolume.isReplica().get())
                                                    && xVolume.getUsableSpace().get() * 0.90 > length) {

                                                return createWriteStream(
                                                        nodes,
                                                        persistentServiceDef,
                                                        length,
                                                        messageDigestFactories,
                                                        xVolume)
                                                        .map(nodeWriteStreamOptional -> {
                                                            if (nodeWriteStreamOptional.isPresent()) {
                                                                NodeWriteStreamBlob nodeWriteStream = nodeWriteStreamOptional.get();
                                                                writeStreamsByNode.put(key, nodeWriteStream);
                                                                if (LOGGER.isDebugEnabled()) {
                                                                    LOGGER.debug("Connected to volume " + xVolume.toJsonObject().encodePrettily());
                                                                }
                                                            }
                                                            return writeStreamsByNode.size() < numberToCollect;
                                                        });

                                            }

                                            return just(true);

                                        }
                                )
                                        .map(aBoolean -> writeStreamsByNode.size() < numberToCollect)
                )
                        .map(aBoolean -> writeStreamsByNode);
            }
        });
    }

    protected Observable<Optional<NodeWriteStreamBlob>> createWriteStream(Nodes nodes, PersistentServiceDef persistentServiceDef, long length, MessageDigestFactory[] messageDigestFactories, XVolume<? extends XVolume> xVolume) {
        return defer(() -> {
            Iterable<? extends XNode<? extends XNode>> candidateConnections = nodes.candidateConnections(vertxContext, persistentServiceDef);
            AtomicReference<NodeWriteStreamBlob> match = new AtomicReference<>();
            return iterate(
                    candidateConnections,
                    xNode -> {
                        String volumeId = xVolume.getId().get();
                        return xNode.canPut(volumeId)
                                .flatMap(canPut -> {
                                    if (TRUE.equals(canPut)) {
                                        return xNode.createWriteStream(volumeId, length, messageDigestFactories)
                                                .map(nodeWriteStreamBlob -> {
                                                    match.set(nodeWriteStreamBlob);
                                                    return false;
                                                });
                                    } else {
                                        return Observable.just(true);
                                    }
                                })
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Handling Connect Failure to service " + xNode.getHostAndPort() + ", volume " + xVolume, throwable);
                                    return Observable.just(true);
                                });
                    })
                    .map(aborted -> fromNullable(match.get()));
        });
    }

    protected abstract static class Key {

        public abstract String getIdentity();

        public static Key nodeIdentity(PersistentServiceDef persistentServiceDef) {
            return new NodeKey(persistentServiceDef.getId());
        }

        public static Key volumeIdentity(PersistentServiceDef persistentServiceDef, XVolume<?> xVolume) {
            return new VolumeKey(persistentServiceDef.getId(), xVolume.getId().get());
        }
    }

    protected static class NodeKey extends Key {
        private final String nodeId;

        public NodeKey(String nodeId) {
            this.nodeId = nodeId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NodeKey)) return false;
            NodeKey nodeKey = (NodeKey) o;
            return Objects.equals(nodeId, nodeKey.nodeId);
        }

        @Override
        public int hashCode() {
            return hash(nodeId);
        }

        @Override
        public String getIdentity() {
            return "NodeKey{" +
                    "nodeId='" + nodeId + '\'' +
                    "} ";
        }
    }

    protected static class VolumeKey extends Key {

        private final String nodeId;
        private final String volumeId;

        public VolumeKey(String nodeId, String volumeId) {
            this.nodeId = nodeId;
            this.volumeId = volumeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof VolumeKey)) return false;
            VolumeKey volumeKey = (VolumeKey) o;
            return Objects.equals(nodeId, volumeKey.nodeId) &&
                    Objects.equals(volumeId, volumeKey.volumeId);
        }

        @Override
        public int hashCode() {
            return hash(nodeId, volumeId);
        }

        @Override
        public String getIdentity() {
            return "VolumeKey{" +
                    "nodeId='" + nodeId + '\'' +
                    ", volumeId='" + volumeId + '\'' +
                    "} ";
        }
    }

    protected String join(Map<Key, NodeWriteStreamBlob> map) {
        Iterable<String> nodes =
                FluentIterable.from(map.entrySet())
                        .transform(input -> {
                            String id = input.getKey().getIdentity();
                            XNode node = input.getValue().getNode();
                            HostAndPort hostAndPort = node.getHostAndPort();
                            return id + " (" + hostAndPort.toString() + ")";
                        });
        return on(',').join(nodes);
    }


}
