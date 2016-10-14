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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.filesystem.volume.HeaderBlob;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.filesystem.volume.Volume;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.filesystem.volume.WriteStreamBlob;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.DigestReadStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.rx.Defer;
import org.sfs.rx.HandleServerToBusy;
import org.sfs.rx.Holder2;
import org.sfs.util.MessageDigestFactory;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Joiner.on;
import static com.google.common.base.Optional.of;
import static com.google.common.net.HostAndPort.fromHost;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.sfs.rx.Defer.just;
import static rx.Observable.defer;

public class LocalNode extends AbstractNode {

    private static final Logger LOGGER = getLogger(LocalNode.class);
    private final VolumeManager volumeManager;
    private final HostAndPort hostAndPort;
    private final VertxContext<Server> vertxContext;

    public LocalNode(VertxContext<Server> vertxContext, VolumeManager volumeManager) {
        this.vertxContext = vertxContext;
        this.volumeManager = volumeManager;
        this.hostAndPort = fromHost("127.0.0.1");
    }

    @Override
    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public Observable<Optional<TransientServiceDef>> getNodeStats() {
        NodeStats nodeStats = vertxContext.verticle().getNodeStats();
        return Defer.just(nodeStats.getStats());
    }

    @Override
    public Observable<Optional<DigestBlob>> checksum(String volumeId, long position, Optional<Long> oOffset, Optional<Long> oLength, MessageDigestFactory... messageDigestFactories) {
        return defer(() -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("begin head {volumeId=%s,position=%d,messageDigests=%s}", volumeId, position, on(',').join(messageDigestFactories)));
            }
            return createReadStream(volumeId, position, oOffset, oLength)
                    .flatMap(oReadStreamBlob -> {
                        if (oReadStreamBlob.isPresent()) {
                            ReadStreamBlob readStreamBlob = oReadStreamBlob.get();
                            final DigestBlob digestBlob = new DigestBlob(readStreamBlob);
                            if (messageDigestFactories.length > 0) {
                                BufferEndableWriteStream nullWriteStream = new NullEndableWriteStream();
                                final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(nullWriteStream, messageDigestFactories);
                                return readStreamBlob.produce(digestWriteStream)
                                        .map(aVoid -> {
                                            for (Holder2<MessageDigestFactory, byte[]> digest : digestWriteStream.digests()) {
                                                digestBlob.withDigest(digest.value0(), digest.value1());
                                            }
                                            return of(digestBlob);
                                        });
                            } else {
                                return just(of(digestBlob));
                            }
                        } else {
                            return just(Optional.<DigestBlob>absent());
                        }
                    })
                    .map(digestBlobOptional -> {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("end head {volumeId=%s,position=%d,messageDigests=%s} = %s", volumeId, position, on(',').join(messageDigestFactories), digestBlobOptional));
                        }
                        return digestBlobOptional;
                    })
                    .onErrorResumeNext(new HandleServerToBusy<>());
        });

    }

    @Override
    public Observable<Optional<HeaderBlob>> acknowledge(String volumeId, long position) {
        return defer(() -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("begin acknowledge {volumeId=%s,position=%d}", volumeId, position));
            }
            Volume volume = volumeManager.get(volumeId).get();
            return volume.acknowledge(vertxContext.vertx(), position)
                    .map(headerBlobOptional -> {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("end acknowledge {volumeId=%s,position=%d}", volumeId, position));
                        }
                        return headerBlobOptional;
                    })
                    .onErrorResumeNext(new HandleServerToBusy<>());
        });
    }

    @Override
    public Observable<Optional<HeaderBlob>> delete(String volumeId, long position) {
        return defer(() -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("delete {volumeId=%s,position=%d}", volumeId, position));
            }
            Volume volume = volumeManager.get(volumeId).get();
            return volume.delete(vertxContext.vertx(), position)
                    .onErrorResumeNext(new HandleServerToBusy<>());
        });
    }

    @Override
    public Observable<Optional<ReadStreamBlob>> createReadStream(String volumeId, long position, Optional<Long> offset, Optional<Long> length) {
        return defer(() -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("createReadStream {volumeId=%s,position=%d,offset=%s,length=%s}", volumeId, position, offset, length));
            }
            Volume volume = volumeManager.get(volumeId).get();
            return volume.getDataStream(vertxContext.vertx(), position, offset, length)
                    .onErrorResumeNext(new HandleServerToBusy<>());
        });
    }

    @Override
    public Observable<Boolean> canReadVolume(String volumeId) {
        return canWriteVolume(volumeId);
    }

    @Override
    public Observable<Boolean> canWriteVolume(String volumeId) {
        return defer(() -> {
            LOGGER.debug("Volume Manager is Open " + volumeManager.isOpen());
            if (volumeManager.isOpen()) {
                Optional<Volume> oVolume = volumeManager.get(volumeId);
                LOGGER.debug("Volume is " + oVolume);
                if (oVolume.isPresent()) {
                    Volume volume = oVolume.get();
                    Volume.Status status = volume.status();
                    LOGGER.debug("Volume status is " + status);
                    if (Volume.Status.STARTED.equals(status)) {
                        return Defer.just(true);
                    }
                }
            }
            return Defer.just(false);
        });
    }

    @Override
    public Observable<NodeWriteStreamBlob> createWriteStream(String volumeId, long length, final MessageDigestFactory... messageDigestFactories) {
        LocalNode _this = this;
        return defer(() -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("createWriteStream {volumeId=%s,length=%d,messageDigests=%s}", volumeId, length, on(',').join(messageDigestFactories)));
            }
            final Volume volume = volumeManager.get(volumeId).get();
            return volume.putDataStream(vertxContext.vertx(), length)
                    .map((Func1<WriteStreamBlob, NodeWriteStreamBlob>) writeStreamBlob -> new NodeWriteStreamBlob(_this) {
                        @Override
                        public Observable<DigestBlob> consume(ReadStream<Buffer> src) {
                            DigestReadStream digestWriteStream = new DigestReadStream(src, messageDigestFactories);
                            return writeStreamBlob.consume(digestWriteStream)
                                    .map(aVoid -> {
                                        DigestBlob digestBlob =
                                                new DigestBlob(writeStreamBlob.getVolume(),
                                                        writeStreamBlob.getPosition(),
                                                        writeStreamBlob.getLength());
                                        for (MessageDigestFactory messageDigestFactory : messageDigestFactories) {
                                            digestBlob.withDigest(messageDigestFactory, digestWriteStream.getDigest(messageDigestFactory).get());
                                        }
                                        return digestBlob;
                                    });
                        }
                    });
        }).onErrorResumeNext(new HandleServerToBusy<>());
    }

    @Override
    public String toString() {
        return "LocalNode{" +
                "hostAndPort=" + hostAndPort +
                '}';
    }
}
