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
import com.google.common.cache.Cache;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.container.LoadContainer;
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.all.blobreference.AcknowledgeBlobReference;
import org.sfs.nodes.all.blobreference.DeleteBlobReference;
import org.sfs.nodes.all.blobreference.VerifyBlobReference;
import org.sfs.nodes.all.segment.RebalanceSegment;
import org.sfs.nodes.compute.object.PruneObject;
import org.sfs.rx.Defer;
import org.sfs.rx.ResultMemoizeHandler;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;
import rx.Subscriber;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchHitMaintainObjectEndableWrite.class);
    private final ClusterInfo clusterInfo;

    private final Cache<String, PersistentAccount> accountCache =
            newBuilder()
                    .maximumSize(100)
                    .build();

    private final Cache<String, PersistentContainer> containerCache =
            newBuilder()
                    .maximumSize(100)
                    .build();

    private List<TransientServiceDef> dataNodes;


    public SearchHitMaintainObjectEndableWrite(VertxContext<Server> vertxContext) {
        super(vertxContext);
        this.clusterInfo = vertxContext.verticle().getClusterInfo();
        this.dataNodes =
                from(clusterInfo.getDataNodes())
                        .transform(TransientServiceDef::copy)
                        .toList();
    }

    @Override
    protected Observable<Optional<JsonObject>> transform(final JsonObject data, String id, long version) {
        return toPersistentObject(data, id, version)
                // delete versions that are to old to attempt verification,ack and rebalance
                .concatMapDelayError(this::deleteOldUnAckdVersions)
                // attempt to delete versions that need deleting
                .concatMapDelayError(this::pruneObject)
                // verifyAck before rebalance
                // so that in cases where the verify/ack
                // failed to persist to the index
                // we're able to recreate the state
                // needed for rebalancing
                .concatMapDelayError(this::verifyAck)
                // rebalance the objects, including the ones that were just re-verified
                // disable rebalance because more testing is needed
                .concatMapDelayError(this::reBalance)
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
        ResultMemoizeHandler<PersistentObject> handler = new ResultMemoizeHandler<>();
        just(persistentObject)
                .concatMapDelayError(persistentObject1 -> Observable.from(persistentObject1.getVersions()))
                .filter(version -> !version.isDeleted())
                .concatMapDelayError(transientVersion -> Observable.from(transientVersion.getSegments()))
                .filter(transientSegment -> !transientSegment.isTinyData())
                .concatMapDelayError(transientSegment -> Observable.from(transientSegment.getBlobs()))
                .filter(transientBlobReference -> !transientBlobReference.isDeleted())
                .concatMapDelayError(transientBlobReference -> {
                            boolean alreadyAckd = transientBlobReference.isAcknowledged();
                            Optional<Integer> oVerifyFailCount = transientBlobReference.getVerifyFailCount();
                            int verifyFailCount = oVerifyFailCount.isPresent() ? oVerifyFailCount.get() : 0;
                            return just(transientBlobReference)
                                    .concatMapDelayError(new VerifyBlobReference(vertxContext))
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
                .subscribe(new Subscriber<Boolean>() {
                    @Override
                    public void onStart() {
                        request(1);
                    }

                    @Override
                    public void onCompleted() {
                        handler.complete(persistentObject);
                    }

                    @Override
                    public void onError(Throwable e) {
                        handler.fail(e);
                    }

                    @Override
                    public void onNext(Boolean aBoolean) {
                        request(1);
                    }
                });
        return Observable.create(handler.subscribe);
    }

    protected Observable<PersistentObject> reBalance(PersistentObject persistentObject) {
        ResultMemoizeHandler<PersistentObject> handler = new ResultMemoizeHandler<>();
        just(persistentObject)
                .concatMapDelayError(persistentObject1 -> Observable.from(persistentObject1.getVersions()))
                .filter(version -> !version.isDeleted())
                .concatMapDelayError(transientVersion -> Observable.from(transientVersion.getSegments()))
                .concatMapDelayError(transientSegment ->
                        just(transientSegment)
                                .flatMap(new RebalanceSegment(vertxContext, dataNodes))
                                .map(reBalanced -> (Void) null))
                .subscribe(new Subscriber<Void>() {

                    @Override
                    public void onStart() {
                        request(1);
                    }

                    @Override
                    public void onCompleted() {
                        handler.complete(persistentObject);
                    }

                    @Override
                    public void onError(Throwable e) {
                        handler.fail(e);
                    }

                    @Override
                    public void onNext(Void aVoid) {
                        request(1);
                    }
                });
        return Observable.create(handler.subscribe);
    }

    protected Observable<PersistentObject> pruneObject(PersistentObject persistentObject) {
        return just(persistentObject)
                .concatMapDelayError(new PruneObject(vertxContext))
                .map(modified -> persistentObject);
    }

    protected Observable<PersistentObject> deleteOldUnAckdVersions(PersistentObject persistentObject) {
        return defer(() -> {
            long now = currentTimeMillis();
            ResultMemoizeHandler<PersistentObject> handler = new ResultMemoizeHandler<>();
            empty()
                    .filter(aVoid -> now - persistentObject.getUpdateTs().getTimeInMillis() >= CONSISTENCY_THRESHOLD)
                    .map(aVoid -> persistentObject)
                    .concatMapDelayError(persistentObject1 -> Observable.from(persistentObject1.getVersions()))
                    // don't bother trying to delete old versions that are marked as
                    // deleted since we have another method that will take care of that
                    // at some point in the future
                    .filter(version -> !version.isDeleted())
                    .concatMapDelayError(transientVersion -> Observable.from(transientVersion.getSegments()))
                    .filter(transientSegment -> !transientSegment.isTinyData())
                    .concatMapDelayError(transientSegment -> Observable.from(transientSegment.getBlobs()))
                    .filter(transientBlobReference -> !transientBlobReference.isAcknowledged())
                    .filter(transientBlobReference -> {
                        Optional<Integer> oVerifyFailCount = transientBlobReference.getVerifyFailCount();
                        return oVerifyFailCount.isPresent() && oVerifyFailCount.get() >= VERIFY_RETRY_COUNT;
                    })
                    .concatMapDelayError(transientBlobReference ->
                            just(transientBlobReference)
                                    .flatMap(new DeleteBlobReference(vertxContext))
                                    .filter(deleted -> deleted)
                                    .map(deleted -> {
                                        transientBlobReference.setDeleted(deleted);
                                        return (Void) null;
                                    }))
                    .onErrorResumeNext(throwable -> {
                        LOGGER.warn("Handling Error", throwable);
                        return Defer.empty();
                    })
                    .subscribe(new Subscriber<Void>() {

                        @Override
                        public void onStart() {
                            request(1);
                        }

                        @Override
                        public void onCompleted() {
                            handler.complete(persistentObject);
                        }

                        @Override
                        public void onError(Throwable e) {
                            handler.fail(e);
                        }

                        @Override
                        public void onNext(Void aVoid) {
                            request(1);
                        }
                    });
            return Observable.create(handler.subscribe);
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

    protected Iterable<TransientServiceDef> getDataNodes() {
        return clusterInfo.getDataNodes();
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
