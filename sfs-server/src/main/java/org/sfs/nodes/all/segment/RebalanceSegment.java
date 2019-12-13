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

package org.sfs.nodes.all.segment;

import com.google.common.base.Optional;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.io.PipedEndableWriteStream;
import org.sfs.io.PipedReadStream;
import org.sfs.nodes.Nodes;
import org.sfs.nodes.VolumeReplicaGroup;
import org.sfs.nodes.all.blobreference.DeleteBlobReference;
import org.sfs.rx.Defer;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.TransientBlobReference;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.concat;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Math.abs;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.rx.RxHelper.iterate;
import static org.sfs.util.MessageDigestFactory.SHA512;
import static rx.Observable.just;

public class RebalanceSegment implements Func1<TransientSegment, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(RebalanceSegment.class);
    private VertxContext<Server> vertxContext;
    private Nodes nodes;
    private List<TransientServiceDef> dataNodes;
    private Vertx vertx;

    public RebalanceSegment(VertxContext<Server> vertxContext, List<TransientServiceDef> copyOfDataNodes) {
        this.vertxContext = vertxContext;
        this.nodes = vertxContext.verticle().nodes();
        this.dataNodes = copyOfDataNodes;
        this.vertx = vertxContext.vertx();
    }

    @Override
    public Observable<Boolean> call(final TransientSegment transientSegment) {
        if (transientSegment.isTinyData()) {
            return just(true);
        } else {
            return Defer.aVoid()
                    .flatMap(aVoid -> reBalance(transientSegment));
        }
    }


    protected Observable<Boolean> reBalance(TransientSegment transientSegment) {
        List<TransientBlobReference> existingObjectCopies =
                from(transientSegment.verifiedAckdBlobs())
                        .filter(input -> {
                            Optional<Integer> verifyFailCount = input.getVerifyFailCount();
                            return !verifyFailCount.isPresent() || verifyFailCount.get() <= 0;
                        })
                        .toList();

        PersistentContainer container = transientSegment.getParent().getParent().getParent();

        int numberOfExpectedCopies = container.computeNumberOfObjectCopies(nodes);

        checkState(numberOfExpectedCopies >= 1, "Number of object copies must be greater >= 1");

        int numberOfExistingCopies = existingObjectCopies.size();

        int numberOfCopiesNeeded = numberOfExpectedCopies - numberOfExistingCopies;

        return Defer.aVoid()
                .flatMap(aVoid -> {
                    if (numberOfCopiesNeeded < 0) {
                        return balanceDown(existingObjectCopies, abs(numberOfCopiesNeeded))
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Handling Balance Down Replicas Exception", throwable);
                                    return Defer.just(false);
                                });
                    } else {
                        return Defer.just(false);
                    }
                })
                .flatMap(balancedDown -> {
                    if (numberOfCopiesNeeded > 0) {
                        Set<String> usedVolumeIds =
                                from(concat(existingObjectCopies))
                                        .transform(input -> input.getVolumeId().get())
                                        .toSet();
                        return balanceUp(transientSegment, usedVolumeIds, numberOfCopiesNeeded)
                                .map(balancedUp -> balancedDown || balancedUp)
                                .onErrorResumeNext(throwable -> {
                                    LOGGER.error("Handling Balance Up Exception", throwable);
                                    return Defer.just(balancedDown);
                                });
                    } else {
                        return Defer.just(balancedDown);
                    }
                });
    }

    protected Observable<Boolean> balanceDown(List<TransientBlobReference> blobs, int delta) {
        checkState(delta > 0, "Delta must be greater than 0");
        checkState(blobs.size() >= delta, "Number of blobs must be >= %s but was %s", delta, blobs.size());

        AtomicInteger counter = new AtomicInteger(0);
        return iterate(
                vertx,
                blobs,
                transientBlobReference ->
                        just(transientBlobReference)
                                .flatMap(new DeleteBlobReference(vertxContext))
                                .doOnNext(deleted -> {
                                    if (Boolean.TRUE.equals(deleted)) {
                                        transientBlobReference.setDeleted(deleted);
                                        counter.incrementAndGet();
                                    }
                                })
                                .map(deleted -> counter.get() < delta))
                .map(aborted -> counter.get() > 0);
    }

    protected Observable<Boolean> balanceUp(TransientSegment transientSegment, Set<String> usedVolumeIds, int numberOfCopiesNeeded) {
        return Defer.just(transientSegment)
                .flatMap(new GetSegmentReadStream(vertxContext, true))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(holder -> {
                    ReadStreamBlob readStreamBlob = holder.value1();

                    PersistentContainer container = transientSegment.getParent().getParent().getParent();

                    VolumeReplicaGroup volumeReplicaGroup =
                            new VolumeReplicaGroup(vertxContext, numberOfCopiesNeeded, container.computeWriteConsistency(nodes))
                                    .setAllowSameNode(nodes.isAllowSameNode())
                                    .setExcludeVolumeIds(usedVolumeIds);

                    PipedReadStream pipedReadStream = new PipedReadStream();
                    PipedEndableWriteStream pipedEndableWriteStream = new PipedEndableWriteStream(pipedReadStream);
                    Observable<Void> producer = readStreamBlob.produce(pipedEndableWriteStream);

                    Observable<List<DigestBlob>> consumer = volumeReplicaGroup.consume(readStreamBlob.getLength(), SHA512, pipedReadStream);

                    return combineSinglesDelayError(producer, consumer, (aVoid, digestBlobs) -> {
                        for (DigestBlob digestBlob : digestBlobs) {
                            transientSegment.newBlob()
                                    .setVolumeId(digestBlob.getVolume())
                                    .setPosition(digestBlob.getPosition())
                                    .setReadLength(digestBlob.getLength())
                                    .setReadSha512(digestBlob.getDigest(SHA512).get());
                        }
                        return null;
                    });

                })
                .map(aVoid -> transientSegment)
                // Don't ack the segments since writing these to the index
                // is being done as part of a bulk update. The next run
                // of the bulk update will see these records that are not ackd
                // and will ack them if they can be verified. If these
                // records where ackd here it would be possible for volumes
                // to end up with records that are marked as ackd in the volume
                // but not recorded in the index. This strategy allows the volume garabge collector
                // to purge the data from its local store if this index update
                // fails to persist
                .map(transientSegment1 -> true)
                .singleOrDefault(false);
    }
}

