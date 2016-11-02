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
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.compute.object.PruneObject;
import org.sfs.rx.RxHelper;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.Scheduler;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static org.sfs.rx.Defer.just;
import static rx.Observable.defer;

public class SearchHitDestroyObjectEndableWrite extends AbstractBulkUpdateEndableWriteStream {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchHitDestroyObjectEndableWrite.class);
    private final ClusterInfo clusterInfo;

    private final CachedAccount cachedAccount;
    private final CachedContainer cachedContainer;

    private final Scheduler scheduler;


    public SearchHitDestroyObjectEndableWrite(VertxContext<Server> vertxContext) {
        super(vertxContext);
        this.clusterInfo = vertxContext.verticle().getClusterInfo();
        this.scheduler = RxHelper.scheduler(vertxContext.vertx());
        this.cachedAccount = new CachedAccount(vertxContext);
        this.cachedContainer = new CachedContainer(vertxContext);
    }

    @Override
    protected Observable<Optional<JsonObject>> transform(final JsonObject data, String id, long version) {
        return toPersistentObject(data, id, version)
                .doOnNext(persistentObject -> {
                    for (TransientVersion transientVersion : persistentObject.getVersions()) {
                        transientVersion.setDeleted(Boolean.TRUE);
                    }
                })
                // attempt to delete versions that need deleting
                .flatMap(this::pruneObject)
                .timeout(3, TimeUnit.MINUTES, Observable.error(new RuntimeException("Timeout on pruneObject " + data.encodePrettily())), scheduler)
                .map(persistentObject -> {
                    if (persistentObject.getVersions().isEmpty()) {
                        return absent();
                    } else {
                        JsonObject jsonObject = persistentObject.toJsonObject();
                        return of(jsonObject);
                    }
                });
    }


    protected Observable<PersistentObject> pruneObject(PersistentObject persistentObject) {
        return just(persistentObject)
                .flatMap(new PruneObject(vertxContext))
                .map(modified -> persistentObject);
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
