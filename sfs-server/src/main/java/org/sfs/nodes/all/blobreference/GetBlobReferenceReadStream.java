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

package org.sfs.nodes.all.blobreference;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.XNode;
import org.sfs.rx.Defer;
import org.sfs.vo.Segment;
import org.sfs.vo.TransientBlobReference;
import rx.Observable;
import rx.functions.Func1;

import java.util.Arrays;

import static com.google.common.base.Optional.absent;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.FALSE;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.MessageDigestFactory.SHA512;

public class GetBlobReferenceReadStream implements Func1<TransientBlobReference, Observable<Optional<ReadStreamBlob>>> {

    private static final Logger LOGGER = getLogger(GetBlobReferenceReadStream.class);
    private VertxContext<Server> vertxContext;
    private boolean verifyChecksum = false;

    public GetBlobReferenceReadStream(VertxContext<Server> vertxContext, boolean verifyChecksum) {
        this.vertxContext = vertxContext;
        this.verifyChecksum = verifyChecksum;
    }

    @Override
    public Observable<Optional<ReadStreamBlob>> call(TransientBlobReference transientBlobReference) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin getreadstream blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition());
        }
        ClusterInfo clusterInfo = vertxContext.verticle().getClusterInfo();
        Segment<? extends Segment> segment = transientBlobReference.getSegment();
        Optional<byte[]> writeSha512 = segment.getWriteSha512();
        Optional<Long> writeLength = segment.getWriteLength();
        return just(transientBlobReference)
                .filter(transientBlobReference1 -> transientBlobReference1.getVolumeId().isPresent() && transientBlobReference1.getPosition().isPresent())
                .flatMap(transientBlobReference1 -> {
                    String volumeId = transientBlobReference1.getVolumeId().get();
                    Long position = transientBlobReference1.getPosition().get();
                    Optional<XNode> oXNode = clusterInfo.getNodesForVolume(vertxContext, volumeId);
                    if (!oXNode.isPresent()) {
                        LOGGER.warn("No nodes contain volume " + volumeId);
                        return Defer.just(Optional.<ReadStreamBlob>absent());
                    } else {
                        XNode xNode = oXNode.get();
                        if (verifyChecksum) {
                            return xNode.checksum(volumeId, position, absent(), absent(), SHA512)
                                    .flatMap(digestBlobOptional -> {
                                        if (digestBlobOptional.isPresent()) {
                                            DigestBlob digestBlob = digestBlobOptional.get();
                                            byte[] expectedSha512 = digestBlob.getDigest(SHA512).get();
                                            Long expectedLength = digestBlob.getLength();
                                            Optional<byte[]> oExistingSha512 = transientBlobReference1.getReadSha512();
                                            Optional<Long> oExistingLength = transientBlobReference1.getReadLength();
                                            boolean sha512Match = oExistingSha512.isPresent() ? Arrays.equals(expectedSha512, oExistingSha512.get()) : FALSE;
                                            boolean lengthMatch = oExistingLength.isPresent() ? oExistingLength.get().equals(expectedLength) : FALSE;
                                            if (sha512Match && lengthMatch
                                                    && Arrays.equals(writeSha512.get(), expectedSha512)
                                                    && writeLength.get().equals(expectedLength)) {
                                                return xNode.createReadStream(volumeId, position, absent(), absent());
                                            }
                                        }
                                        return Defer.just(Optional.<ReadStreamBlob>absent());
                                    });
                        } else {
                            return xNode.createReadStream(volumeId, position, absent(), absent());
                        }
                    }
                })
                .singleOrDefault(Optional.absent())
                .onErrorResumeNext(throwable -> {
                    LOGGER.error("getreadstream fail blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition(), throwable);
                    return Defer.just(Optional.absent());
                })
                .doOnNext(readStreamBlobOptional -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("end getreadstream blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition() + ", readstreamblob=" + readStreamBlobOptional);
                    }
                });

    }
}