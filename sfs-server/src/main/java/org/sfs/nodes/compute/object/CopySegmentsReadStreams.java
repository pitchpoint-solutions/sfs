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

import com.google.common.base.Optional;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.encryption.ContainerKeys;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.NoEndEndableWriteStream;
import org.sfs.nodes.all.segment.GetSegmentReadStream;
import org.sfs.vo.Segment;
import org.sfs.vo.TransientBlobReference;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.XVersion;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.sfs.io.AsyncIO.end;
import static org.sfs.rx.Defer.just;
import static org.sfs.rx.RxHelper.iterate;
import static org.sfs.vo.Segment.SegmentCipher;

public class CopySegmentsReadStreams implements Func1<Iterable<TransientSegment>, Observable<Iterable<TransientSegment>>> {

    private static final Logger LOGGER = getLogger(CopySegmentsReadStreams.class);
    private final VertxContext<Server> vertxContext;
    private final BufferEndableWriteStream writeStream;

    public CopySegmentsReadStreams(VertxContext<Server> vertxContext, BufferEndableWriteStream writeStream) {
        this.vertxContext = vertxContext;
        this.writeStream = writeStream;
    }

    @Override
    public Observable<Iterable<TransientSegment>> call(Iterable<TransientSegment> transientSegments) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin copy segment read streams");
        }
        return iterate(
                transientSegments,
                transientSegment -> {
                    if (!transientSegment.isTinyData()) {
                        return just(transientSegment)
                                .flatMap(new GetSegmentReadStream(vertxContext))
                                .doOnNext(oHolder -> {
                                    if (!oHolder.isPresent()) {
                                        checkState(oHolder.isPresent(), format("Failed to find ReadStream for segment %d from object %s", transientSegment.getId(), transientSegment.getParent().getParent().toJsonObject().encodePrettily()));
                                    }
                                })
                                .map(Optional::get)
                                .flatMap(holder -> {
                                    TransientBlobReference transientBlobReference = holder.value0();
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("begin copy of blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition());
                                    }
                                    return prepareWriteStream(new NoEndEndableWriteStream(writeStream), transientSegment)
                                            .flatMap(writeStream -> holder.value1().produce(writeStream))
                                            .doOnNext(aVoid -> {
                                                if (LOGGER.isDebugEnabled()) {
                                                    LOGGER.debug("end copy of blob reference object=" + transientBlobReference.getSegment().getParent().getParent().getId() + ", version=" + transientBlobReference.getSegment().getParent().getId() + ", segment=" + transientBlobReference.getSegment().getId() + ", volume=" + transientBlobReference.getVolumeId() + ", position=" + transientBlobReference.getPosition());
                                                }
                                            });
                                })
                                .map(aVoid -> true);
                    } else {
                        Buffer tinyData = buffer(transientSegment.getTinyData());
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("begin copy of blob reference object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
                        }
                        return prepareWriteStream(new NoEndEndableWriteStream(writeStream), transientSegment)
                                .flatMap(writeStream -> end(tinyData, writeStream))
                                .doOnNext(aVoid -> {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("bend copy of blob reference object=" + transientSegment.getParent().getParent().getId() + ", version=" + transientSegment.getParent().getId() + ", segment=" + transientSegment.getId());
                                    }
                                })
                                .map(aVoid -> true);
                    }
                }
        )
                .map(_continue -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("end copy segment read streams");
                    }
                    return transientSegments;
                });
    }

    public Observable<BufferEndableWriteStream> prepareWriteStream(
            BufferEndableWriteStream delegateWriteStream, Segment<? extends Segment> segment) {

        final XVersion<? extends XVersion> transientVersion = segment.getParent();
        Optional<Boolean> oServerSideEncryption = transientVersion.getServerSideEncryption();
        boolean serverSideEncryption = oServerSideEncryption.isPresent() && TRUE.equals(oServerSideEncryption.get());

        if (serverSideEncryption) {

            Optional<SegmentCipher> oSegmentCipher = segment.getSegmentCipher();
            SegmentCipher segmentCipher = oSegmentCipher.get();

            Optional<String> oContainerKeyId = segmentCipher.getContainerKeyId();
            checkState(oContainerKeyId.isPresent(), "SegmentCipher missing ContainerKeyId for Object %s", transientVersion.getId());
            String containerKeyId = oContainerKeyId.get();

            Optional<byte[]> oSalt = segmentCipher.getSalt();
            checkState(oSalt.isPresent(), "SegmentCipher missing salt for Object %s", transientVersion.getId());
            final byte[] salt = oSalt.get();

            ContainerKeys containerKeys = vertxContext.verticle().containerKeys();

            return containerKeys.algorithm(vertxContext, transientVersion.getParent().getParent(), containerKeyId, salt)
                    .map(keyResponse -> keyResponse.getData().decrypt(delegateWriteStream));
        } else {
            return Observable.just(delegateWriteStream);
        }
    }
}
