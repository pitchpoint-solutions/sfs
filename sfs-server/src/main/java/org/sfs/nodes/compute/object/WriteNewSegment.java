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

package org.sfs.nodes.compute.object;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.encryption.ContainerKeys;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.CountingReadStream;
import org.sfs.io.DigestReadStream;
import org.sfs.nodes.Nodes;
import org.sfs.nodes.VolumeReplicaGroup;
import org.sfs.nodes.XNode;
import org.sfs.rx.Holder2;
import org.sfs.util.MessageDigestFactory;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.filesystem.volume.VolumeV1.TINY_DATA_THRESHOLD;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.util.MessageDigestFactory.MD5;
import static org.sfs.util.MessageDigestFactory.SHA512;
import static org.sfs.vo.Segment.SegmentCipher;

public class WriteNewSegment implements Func1<TransientVersion, Observable<TransientSegment>> {

    private static final Logger LOGGER = getLogger(WriteNewSegment.class);
    private final VertxContext<Server> vertxContext;
    private final ReadStream<Buffer> readStream;

    public WriteNewSegment(VertxContext<Server> vertxContext, ReadStream<Buffer> readStream) {
        this.vertxContext = vertxContext;
        this.readStream = readStream;
    }

    @Override
    public Observable<TransientSegment> call(TransientVersion transientVersion) {
        final PersistentContainer persistentContainer = transientVersion.getParent().getParent();

        final long contentLength = transientVersion.getContentLength().get();
        final boolean serverSideEncryption = transientVersion.useServerSideEncryption();

        final Nodes nodes = vertxContext.verticle().nodes();

        final MessageDigestFactory sha512Digest = SHA512;
        final MessageDigestFactory md5Digest = MD5;

        if (serverSideEncryption) {
            ContainerKeys containerKeys = vertxContext.verticle().containerKeys();

            return containerKeys.preferredAlgorithm(vertxContext, persistentContainer)
                    .flatMap(keyResponse -> {

                        VolumeReplicaGroup volumeReplicaGroup = new VolumeReplicaGroup(vertxContext, nodes.getNumberOfObjectReplicasReplicas())
                                .setAllowSameNode(nodes.isAllowSameNode());

                        long encryptedLength = keyResponse.getData().encryptOutputSize(contentLength);

                        final CountingReadStream clearByteCount = new CountingReadStream(readStream);

                        final DigestReadStream serverObjectDigestReadStream = new DigestReadStream(clearByteCount, md5Digest, sha512Digest);

                        ReadStream<Buffer> cipherWriteStream = keyResponse.getData().encrypt(serverObjectDigestReadStream);

                        final CountingReadStream encryptedByteCount = new CountingReadStream(cipherWriteStream);

                        final DigestReadStream blobDigestReadStream = new DigestReadStream(encryptedByteCount, md5Digest, sha512Digest);

                        if (encryptedLength > TINY_DATA_THRESHOLD) {

                            return volumeReplicaGroup.consume(encryptedLength, sha512Digest, blobDigestReadStream)
                                    .map(holders -> {
                                        SegmentCipher segmentCipher = new SegmentCipher(keyResponse.getKeyId(), keyResponse.getSalt());

                                        final TransientSegment newSegment = transientVersion.newSegment();

                                        newSegment.setWriteSha512(blobDigestReadStream.getDigest(sha512Digest).get())
                                                .setSegmentCipher(segmentCipher)
                                                .setWriteLength(encryptedByteCount.count())
                                                .setReadSha512(serverObjectDigestReadStream.getDigest(sha512Digest).get())
                                                .setReadMd5(serverObjectDigestReadStream.getDigest(md5Digest).get())
                                                .setReadLength(clearByteCount.count())
                                                .setIsTinyData(false);

                                        for (Holder2<XNode, DigestBlob> response : holders) {
                                            DigestBlob digestBlob = response.value1();
                                            newSegment.newBlob()
                                                    .setVolumeId(digestBlob.getVolume())
                                                    .setPosition(digestBlob.getPosition())
                                                    .setReadLength(digestBlob.getLength())
                                                    .setReadSha512(digestBlob.getDigest(sha512Digest).get());
                                        }

                                        return newSegment;
                                    });
                        } else {
                            BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                            return pump(blobDigestReadStream, bufferWriteStream)
                                    .map(aVoid -> {
                                        SegmentCipher segmentCipher = new SegmentCipher(keyResponse.getKeyId(), keyResponse.getSalt());

                                        final TransientSegment newSegment = transientVersion.newSegment();

                                        newSegment.setWriteSha512(blobDigestReadStream.getDigest(sha512Digest).get())
                                                .setSegmentCipher(segmentCipher)
                                                .setWriteLength(encryptedByteCount.count())
                                                .setReadSha512(serverObjectDigestReadStream.getDigest(sha512Digest).get())
                                                .setReadMd5(serverObjectDigestReadStream.getDigest(md5Digest).get())
                                                .setReadLength(clearByteCount.count())
                                                .setIsTinyData(true)
                                                .setTinyData(bufferWriteStream.toBuffer().getBytes());

                                        return newSegment;
                                    });

                        }

                    });
        } else {

            VolumeReplicaGroup volumeReplicaGroup =
                    new VolumeReplicaGroup(vertxContext, nodes.getNumberOfObjectReplicasReplicas())
                            .setAllowSameNode(nodes.isAllowSameNode());

            final CountingReadStream clearByteCount = new CountingReadStream(readStream);
            final DigestReadStream digestReadStream = new DigestReadStream(clearByteCount, md5Digest, sha512Digest);

            if (contentLength > TINY_DATA_THRESHOLD) {

                return volumeReplicaGroup.consume(contentLength, sha512Digest, digestReadStream)
                        .map(holders -> {

                            final TransientSegment newSegment = transientVersion.newSegment();

                            newSegment.setWriteSha512(digestReadStream.getDigest(sha512Digest).get())
                                    .setSegmentCipher(null)
                                    .setWriteLength(clearByteCount.count())
                                    .setReadSha512(digestReadStream.getDigest(sha512Digest).get())
                                    .setReadMd5(digestReadStream.getDigest(md5Digest).get())
                                    .setReadLength(clearByteCount.count())
                                    .setIsTinyData(false);

                            for (Holder2<XNode, DigestBlob> response : holders) {
                                DigestBlob digestBlob = response.value1();
                                newSegment.newBlob()
                                        .setVolumeId(digestBlob.getVolume())
                                        .setPosition(digestBlob.getPosition())
                                        .setReadLength(digestBlob.getLength())
                                        .setReadSha512(digestBlob.getDigest(sha512Digest).get());
                            }

                            return newSegment;
                        });
            } else {
                BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                return pump(digestReadStream, bufferWriteStream)
                        .map(aVoid -> {

                            final TransientSegment newSegment = transientVersion.newSegment();

                            newSegment.setWriteSha512(digestReadStream.getDigest(sha512Digest).get())
                                    .setSegmentCipher(null)
                                    .setWriteLength(clearByteCount.count())
                                    .setReadSha512(digestReadStream.getDigest(sha512Digest).get())
                                    .setReadMd5(digestReadStream.getDigest(md5Digest).get())
                                    .setReadLength(clearByteCount.count())
                                    .setIsTinyData(true)
                                    .setTinyData(bufferWriteStream.toBuffer().getBytes());

                            return newSegment;
                        });
            }

        }

    }
}

