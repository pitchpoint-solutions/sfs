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

package org.sfs.nodes.compute.account;

import com.google.common.base.Optional;
import io.vertx.core.Handler;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.account.PersistAccount;
import org.sfs.elasticsearch.account.UpdateAccount;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.validate.ValidateAccountPath;
import org.sfs.validate.ValidateActionAdmin;
import org.sfs.validate.ValidateOptimisticAccountLock;
import org.sfs.vo.PersistentAccount;
import org.sfs.vo.TransientAccount;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;
import rx.functions.Func1;

import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class PostAccount implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdmin(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateAccountPath())
                .map(objectPath -> objectPath.accountPath().get())
                .flatMap(new LoadAccount(httpServerRequest.vertxContext()))
                .flatMap(new Func1<Optional<PersistentAccount>, Observable<PersistentAccount>>() {
                    @Override
                    public Observable<PersistentAccount> call(Optional<PersistentAccount> oPersistentAccount) {
                        if (oPersistentAccount.isPresent()) {
                            PersistentAccount persistentAccount = oPersistentAccount.get();
                            persistentAccount.merge(httpServerRequest);
                            return just(persistentAccount)
                                    .flatMap(new UpdateAccount(httpServerRequest.vertxContext()))
                                    .map(new ValidateOptimisticAccountLock());
                        } else {
                            return just(TransientAccount.fromSfsRequest(httpServerRequest))
                                    .doOnNext(transientAccount -> {
                                        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();
                                        Optional<TransientServiceDef> currentMaintainerNode =
                                                vertxContext
                                                        .verticle()
                                                        .getClusterInfo()
                                                        .getCurrentMaintainerNode();
                                        if (currentMaintainerNode.isPresent()) {
                                            transientAccount.setNodeId(currentMaintainerNode.get().getId());
                                        }
                                    })
                                    .flatMap(new PersistAccount(httpServerRequest.vertxContext()))
                                    .map(new ValidateOptimisticAccountLock());
                        }
                    }
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<PersistentAccount>(httpServerRequest) {
                    @Override
                    public void onNext(PersistentAccount persistentAccount) {
                        httpServerRequest.response().setStatusCode(HTTP_NO_CONTENT);
                    }
                });

    }
}
