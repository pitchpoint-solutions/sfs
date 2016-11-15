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

package org.sfs.elasticsearch;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.jobs.VerifyRepairAllContainerObjects;
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.all.blobreference.AcknowledgeBlobReference;
import org.sfs.nodes.all.blobreference.DeleteBlobReference;
import org.sfs.nodes.all.blobreference.VerifyBlobReference;
import org.sfs.nodes.all.segment.RebalanceSegment;
import org.sfs.nodes.compute.object.PruneObject;
import org.sfs.rx.Defer;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToType;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientBlobReference;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.TransientServiceDef;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.Scheduler;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.collect.FluentIterable.from;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.currentTimeMillis;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static rx.Observable.defer;

public class SearchHitMaintainObjectEndableWrite extends AbstractBulkUpdateEndableWriteStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchHitMaintainObjectEndableWrite.class);
    private final ClusterInfo clusterInfo;

    private final CachedAccount cachedAccount;
    private final CachedContainer cachedContainer;

    private final List<TransientServiceDef> dataNodes;
    private final Scheduler scheduler;
    private final Set<String> forceRemoveVolumes;


    public SearchHitMaintainObjectEndableWrite(VertxContext<Server> vertxContext, Set<String> forceRemoveVolumes) {
        super(vertxContext);
        this.clusterInfo = vertxContext.verticle().getClusterInfo();
        this.dataNodes =
                from(clusterInfo.getDataNodes())
                        .transform(TransientServiceDef::copy)
                        .toList();
        this.scheduler = RxHelper.scheduler(vertxContext.vertx());
        this.cachedAccount = new CachedAccount(vertxContext);
        this.cachedContainer = new CachedContainer(vertxContext);
        this.forceRemoveVolumes = forceRemoveVolumes;
        if (!forceRemoveVolumes.isEmpty()) {
            LOGGER.info("forceRemoveVolumes: " + forceRemoveVolumes);
        }
    }

    @Override
    protected Observable<Optional<JsonObject>> transform(final JsonObject data, String id, long version) {
        return toPersistentObject(data, id, version)
                .map(this::forceRemoveVolumes)
                // delete versions that are to old to attempt verification,ack and rebalance
                .flatMap(this::deleteOldUnAckdVersions)
                .timeout(3, TimeUnit.MINUTES, Observable.error(new RuntimeException("Timeout on deleteOldUnAckdVersions " + data.encodePrettily())), scheduler)
                // attempt to delete versions that need deleting
                .flatMap(this::pruneObject)
                .timeout(3, TimeUnit.MINUTES, Observable.error(new RuntimeException("Timeout on pruneObject " + data.encodePrettily())), scheduler)
                // verifyAck before rebalance
                // so that in cases where the verify/ack
                // failed to persist to the index
                // we're able to recreate the state
                // needed for rebalancing
                .flatMap(this::verifyAck)
                .timeout(3, TimeUnit.MINUTES, Observable.error(new RuntimeException("Timeout on verifyAck " + data.encodePrettily())), scheduler)
                // rebalance the objects, including the ones that were just re-verified
                // disable rebalance because more testing is needed
                .flatMap(this::reBalance)
                .timeout(3, TimeUnit.MINUTES, Observable.error(new RuntimeException("Timeout on reBalance " + data.encodePrettily())), scheduler)
                .map(persistentObject -> {
                    if (persistentObject.getVersions().isEmpty()) {
                        return absent();
                    } else {
                        JsonObject jsonObject = persistentObject.toJsonObject();
                        return of(jsonObject);
                    }
                });
    }

    protected PersistentObject forceRemoveVolumes(PersistentObject persistentObject) {
        if (!forceRemoveVolumes.isEmpty()) {
            for (TransientVersion version : persistentObject.getVersions()) {
                for (TransientSegment segment : version.getSegments()) {
                    if (!segment.isTinyData()) {
                        Iterator<TransientBlobReference> iterator = segment.getBlobs().iterator();
                        while (iterator.hasNext()) {
                            TransientBlobReference blob = iterator.next();
                            String volumeId = blob.getVolumeId().orNull();
                            if (volumeId != null && forceRemoveVolumes.contains(volumeId)) {
                                iterator.remove();
                            }
                        }
                    }
                }
            }
        }
        return persistentObject;
    }

    protected Observable<PersistentObject> verifyAck(PersistentObject persistentObject) {
        return just(persistentObject)
                .flatMap(persistentObject1 -> Observable.from(persistentObject1.getVersions()))
                .filter(version -> !version.isDeleted())
                .flatMap(transientVersion -> Observable.from(transientVersion.getSegments()))
                .filter(transientSegment -> !transientSegment.isTinyData())
                .flatMap(transientSegment -> Observable.from(transientSegment.getBlobs()))
                .filter(transientBlobReference -> !transientBlobReference.isDeleted())
                .flatMap(transientBlobReference -> {
                            boolean alreadyAckd = transientBlobReference.isAcknowledged();
                            Optional<Integer> oVerifyFailCount = transientBlobReference.getVerifyFailCount();
                            int verifyFailCount = oVerifyFailCount.isPresent() ? oVerifyFailCount.get() : 0;
                            return just(transientBlobReference)
                                    .flatMap(new VerifyBlobReference(vertxContext))
                                    .map(verified -> {
                                        // we do this here to unAck blob refs
                                        // in cases where the referenced volume has somehow
                                        // become corrupted
                                        if (!verified) {
                                            if (verifyFailCount >= VerifyRepairAllContainerObjects.VERIFY_RETRY_COUNT) {
                                                transientBlobReference.setAcknowledged(FALSE);
                                            } else {
                                                transientBlobReference.setVerifyFailCount(verifyFailCount + 1);
                                            }
                                        } else {
                                            transientBlobReference.setVerifyFailCount(0);
                                            transientBlobReference.setAcknowledged(TRUE);
                                        }
                                        return verified;
                                    })
                                    // only call ack on the volume if this blob reference
                                    // was successfully verified and not already ackd since
                                    // there's not sense in re-ack'ing
                                    .filter(verified -> verified && !alreadyAckd)
                                    .map(aVoid -> transientBlobReference)
                                    .flatMap(new AcknowledgeBlobReference(vertxContext));
                        }
                )
                .count()
                .map(new ToType<>(persistentObject));
    }

    protected Observable<PersistentObject> reBalance(PersistentObject persistentObject) {
        return just(persistentObject)
                .flatMap(persistentObject1 -> Observable.from(persistentObject1.getVersions()))
                .filter(version -> !version.isDeleted())
                .flatMap(transientVersion -> Observable.from(transientVersion.getSegments()))
                .flatMap(transientSegment ->
                        just(transientSegment)
                                .flatMap(new RebalanceSegment(vertxContext, dataNodes))
                                .map(reBalanced -> (Void) null))
                .count()
                .map(new ToType<>(persistentObject));
    }

    protected Observable<PersistentObject> pruneObject(PersistentObject persistentObject) {
        return just(persistentObject)
                .flatMap(new PruneObject(vertxContext))
                .map(modified -> persistentObject);
    }

    protected Observable<PersistentObject> deleteOldUnAckdVersions(PersistentObject persistentObject) {
        return defer(() -> {
            long now = currentTimeMillis();
            return aVoid()
                    .filter(aVoid -> now - persistentObject.getUpdateTs().getTimeInMillis() >= VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD)
                    .map(aVoid -> persistentObject)
                    .flatMap(persistentObject1 -> Observable.from(persistentObject1.getVersions()))
                    // don't bother trying to delete old versions that are marked as
                    // deleted since we have another method that will take care of that
                    // at some point in the future
                    .filter(version -> !version.isDeleted())
                    .flatMap(transientVersion -> Observable.from(transientVersion.getSegments()))
                    .filter(transientSegment -> !transientSegment.isTinyData())
                    .flatMap(transientSegment -> Observable.from(transientSegment.getBlobs()))
                    .filter(transientBlobReference -> !transientBlobReference.isAcknowledged())
                    .filter(transientBlobReference -> {
                        Optional<Integer> oVerifyFailCount = transientBlobReference.getVerifyFailCount();
                        return oVerifyFailCount.isPresent() && oVerifyFailCount.get() >= VerifyRepairAllContainerObjects.VERIFY_RETRY_COUNT;
                    })
                    .flatMap(transientBlobReference ->
                            just(transientBlobReference)
                                    .flatMap(new DeleteBlobReference(vertxContext))
                                    .filter(deleted -> deleted)
                                    .map(deleted -> {
                                        if (Boolean.TRUE.equals(deleted)) {
                                            transientBlobReference.setDeleted(deleted);
                                        }
                                        return (Void) null;
                                    }))
                    .onErrorResumeNext(throwable -> {
                        LOGGER.warn("Handling Error", throwable);
                        return Defer.aVoid();
                    })
                    .count()
                    .map(new ToType<>(persistentObject));
        });
    }

    protected Observable<PersistentObject> toPersistentObject(JsonObject jsonObject, String id, long version) {
        return defer(() -> {

            final String accountId = jsonObject.getString("account_id");
            final String containerId = jsonObject.getString("container_id");
            return just(jsonObject)
                    .filter(jsonObject1 -> accountId != null && containerId != null)
                    .flatMap(document -> getAccount(accountId))
                    .flatMap(persistentAccount -> getContainer(persistentAccount, containerId))
                    .map(persistentContainer -> new PersistentObject(persistentContainer, id, version).merge(jsonObject));
        });
    }

    protected Observable<PersistentAccount> getAccount(String accountId) {
        return cachedAccount.get(accountId);
    }

    protected Observable<PersistentContainer> getContainer(PersistentAccount persistentAccount, String containerId) {
        return cachedContainer.get(persistentAccount, containerId);
    }
}
