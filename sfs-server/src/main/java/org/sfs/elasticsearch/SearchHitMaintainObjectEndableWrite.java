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

package org.sfs.elasticsearch;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.container.LoadContainer;
import org.sfs.nodes.Nodes;
import org.sfs.nodes.all.blobreference.AcknowledgeBlobReference;
import org.sfs.nodes.all.blobreference.DeleteBlobReference;
import org.sfs.nodes.all.blobreference.VerifyBlobReference;
import org.sfs.nodes.all.segment.RebalanceSegment;
import org.sfs.nodes.compute.object.PruneObject;
import org.sfs.rx.ToVoid;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.PersistentServiceDef;
import rx.Observable;

import java.util.List;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.collect.FluentIterable.from;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.currentTimeMillis;
import static org.sfs.elasticsearch.object.MaintainObjectsForNode.CONSISTENCY_THRESHOLD;
import static org.sfs.elasticsearch.object.MaintainObjectsForNode.VERIFY_RETRY_COUNT;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static rx.Observable.defer;

public class SearchHitMaintainObjectEndableWrite extends AbstractBulkUpdateEndableWriteStream {

    private final Nodes nodes;

    private final Cache<String, PersistentAccount> accountCache =
            newBuilder()
                    .maximumSize(100)
                    .build();

    private final Cache<String, PersistentContainer> containerCache =
            newBuilder()
                    .maximumSize(100)
                    .build();

    private List<PersistentServiceDef> dataNodes;


    public SearchHitMaintainObjectEndableWrite(VertxContext<Server> vertxContext) {
        super(vertxContext);
        this.nodes = vertxContext.verticle().nodes();
        this.dataNodes =
                from(nodes.getDataNodes(vertxContext))
                        .transform(PersistentServiceDef::copy)
                        .toList();
    }

    @Override
    protected Observable<Optional<JsonObject>> transform(final JsonObject data, String id, long version) {
        return toPersistentObject(data, id, version)
                // delete versions that are to old to attempt verification,ack and rebalance
                .flatMap(this::deleteOldUnAckdVersions)
                // attempt to delete versions that need deleting
                .flatMap(this::pruneObject)
                // verifyAck before rebalance
                // so that in cases where the verify/ack
                // failed to persist to the index
                // we're able to recreate the state
                // needed for rebalancing
                .flatMap(this::verifyAck)
                // rebalance the objects, including the ones that were just re-verified
                // disable rebalance because more testing is needed
                .flatMap(this::reBalance)
                .map(persistentObject -> {
                    if (persistentObject.getVersions().isEmpty()) {
                        return absent();
                    } else {
                        JsonObject jsonObject = persistentObject.toJsonObject();
                        return of(jsonObject);
                    }
                });
    }

    protected Observable<PersistentObject> verifyAck(PersistentObject persistentObject) {
        return just(persistentObject)
                .flatMap(persistentObject1 -> Observable.from(persistentObject1.getVersions()))
                .filter(version -> !version.isDeleted())
                .flatMap(transientVersion -> Observable.from(transientVersion.getSegments()))
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
                                            if (verifyFailCount >= VERIFY_RETRY_COUNT) {
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
                .map(new ToVoid<>())
                .singleOrDefault(null)
                .map(aVoid -> persistentObject);
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
                .map(new ToVoid<>())
                .singleOrDefault(null)
                .map(aVoid -> persistentObject);
    }

    protected Observable<PersistentObject> pruneObject(PersistentObject persistentObject) {
        return just(persistentObject)
                .flatMap(new PruneObject(vertxContext))
                .map(modified -> persistentObject);
    }

    protected Observable<PersistentObject> deleteOldUnAckdVersions(PersistentObject persistentObject) {
        return defer(() -> {
            long now = currentTimeMillis();
            return empty()
                    .filter(aVoid -> now - persistentObject.getUpdateTs().getTimeInMillis() >= CONSISTENCY_THRESHOLD)
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
                        return oVerifyFailCount.isPresent() && oVerifyFailCount.get() >= VERIFY_RETRY_COUNT;
                    })
                    .flatMap(transientBlobReference ->
                            just(transientBlobReference)
                                    .flatMap(new DeleteBlobReference(vertxContext))
                                    .filter(deleted -> deleted)
                                    .map(deleted -> {
                                        transientBlobReference.setDeleted(deleted);
                                        return (Void) null;
                                    }))
                    .count()
                    .map(new ToVoid<>())
                    .singleOrDefault(null)
                    .map(aVoid -> persistentObject);
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

    protected Iterable<PersistentServiceDef> getDataNodes() {
        return nodes.getDataNodes(vertxContext);
    }

    protected Observable<PersistentAccount> getAccount(String accountId) {
        return defer(() -> {
            final PersistentAccount persistentAccount = accountCache.getIfPresent(accountId);
            if (persistentAccount == null) {
                return just(accountId)
                        .flatMap(new LoadAccount(vertxContext))
                        .map(oPersistentAccount -> {
                            if (oPersistentAccount.isPresent()) {
                                accountCache.put(accountId, oPersistentAccount.get());

                            }
                            return oPersistentAccount.get();
                        });
            } else {
                return just(persistentAccount);
            }
        });
    }

    protected Observable<PersistentContainer> getContainer(PersistentAccount persistentAccount, String containerId) {
        return defer(() -> {
            final PersistentContainer persistentContainer = containerCache.getIfPresent(containerId);
            if (persistentContainer == null) {
                return just(containerId)
                        .flatMap(new LoadContainer(vertxContext, persistentAccount))
                        .map(oPersistentContainer -> {
                            if (oPersistentContainer.isPresent()) {
                                containerCache.put(containerId, oPersistentContainer.get());

                            }
                            return oPersistentContainer.get();
                        });
            } else {
                return just(persistentContainer);
            }
        });
    }
}
