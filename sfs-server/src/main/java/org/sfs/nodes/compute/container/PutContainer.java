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

package org.sfs.nodes.compute.container;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.container.CreateObjectIndex;
import org.sfs.elasticsearch.container.LoadContainer;
import org.sfs.elasticsearch.container.PersistContainer;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.Defer;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionContainerCreate;
import org.sfs.validate.ValidateContainerPath;
import org.sfs.validate.ValidateHeaderBetweenInteger;
import org.sfs.validate.ValidateOptimisticContainerLock;
import org.sfs.validate.ValidatePersistentAccountExists;
import org.sfs.validate.ValidatePersistentContainerNotExists;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.TransientContainer;

import static java.lang.Integer.parseInt;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.SfsHttpHeaders.X_SFS_OBJECT_INDEX_REPLICAS;
import static org.sfs.util.SfsHttpHeaders.X_SFS_OBJECT_INDEX_SHARDS;
import static org.sfs.util.SfsHttpHeaders.X_SFS_OBJECT_REPLICAS;
import static org.sfs.vo.ObjectPath.fromSfsRequest;
import static rx.Observable.just;

public class PutContainer implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {
        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();
        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateHeaderBetweenInteger(X_SFS_OBJECT_INDEX_SHARDS, 1, 1024))
                .map(new ValidateHeaderBetweenInteger(X_SFS_OBJECT_INDEX_REPLICAS, 1, 6))
                .map(new ValidateHeaderBetweenInteger(X_SFS_OBJECT_REPLICAS, 1, 6))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateContainerPath())
                .flatMap(objectPath -> {
                    String accountId = objectPath.accountPath().get();
                    String containerId = objectPath.containerPath().get();
                    return just(accountId)
                            .flatMap(new LoadAccount(vertxContext))
                            .map(new ValidatePersistentAccountExists())
                            .flatMap(persistentAccount -> {
                                        TransientContainer transientContainer = TransientContainer.fromSfsRequest(persistentAccount, httpServerRequest)
                                                .setOwnerGuid(httpServerRequest.getUserAndRole().getUser().getId());
                                        return Defer.just(transientContainer)
                                                .flatMap(new ValidateActionContainerCreate(httpServerRequest))
                                                .map(new ToVoid<>())
                                                .map(aVoid -> containerId)
                                                .flatMap(new LoadContainer(vertxContext, persistentAccount))
                                                .map(new ValidatePersistentContainerNotExists())
                                                .map(aVoid -> transientContainer)
                                                .flatMap(new PersistContainer(vertxContext))
                                                .map(new ValidateOptimisticContainerLock())
                                                .flatMap(persistentContainer -> {
                                                    MultiMap headers = httpServerRequest.headers();
                                                    CreateObjectIndex createObjectIndex = new CreateObjectIndex(vertxContext);
                                                    if (headers.contains(X_SFS_OBJECT_INDEX_SHARDS)) {
                                                        int shards = parseInt(headers.get(X_SFS_OBJECT_INDEX_SHARDS));
                                                        createObjectIndex = createObjectIndex.setNumberOfShards(shards);
                                                    }
                                                    if (headers.contains(X_SFS_OBJECT_INDEX_REPLICAS)) {
                                                        int replicas = parseInt(headers.get(X_SFS_OBJECT_INDEX_REPLICAS));
                                                        createObjectIndex = createObjectIndex.setNumberOfReplicas(replicas);
                                                    }
                                                    return createObjectIndex.call(persistentContainer);
                                                });

                                    }
                            );
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<PersistentContainer>(httpServerRequest) {
                    @Override
                    public void onNext(PersistentContainer persistentContainer) {
                        httpServerRequest.response().setStatusCode(HTTP_CREATED);
                    }
                });


    }
}
