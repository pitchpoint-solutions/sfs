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

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.account.ExistsContainers;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.account.RemoveAccount;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.validate.ValidateAccountPath;
import org.sfs.validate.ValidateActionAdmin;
import org.sfs.validate.ValidateOptimisticAccountLock;
import org.sfs.validate.ValidatePersistentAccountExists;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class DeleteAccount implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdmin(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateAccountPath())
                .map(objectPath -> objectPath.accountPath().get())
                .flatMap(new LoadAccount(httpServerRequest.vertxContext()))
                .map(new ValidatePersistentAccountExists())
                .flatMap(persistentAccount ->
                        just(persistentAccount)
                                .flatMap(new ExistsContainers(vertxContext))
                                .doOnNext(existsObjects -> {
                                    if (existsObjects) {
                                        throw new HttpRequestValidationException(HTTP_CONFLICT, new JsonObject()
                                                .put("message", "container is not empty"));
                                    }
                                })
                                .map(existsObjects -> persistentAccount))
                .flatMap(new RemoveAccount(httpServerRequest.vertxContext()))
                .map(new ValidateOptimisticAccountLock())
                .map(new ToVoid<>())
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }
}
