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

package org.sfs.nodes.compute.account;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import org.sfs.SfsRequest;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.account.ListContainers;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.metadata.Metadata;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.validate.ValidateAccountPath;
import org.sfs.validate.ValidateActionAdmin;
import org.sfs.validate.ValidatePersistentAccountExists;
import org.sfs.vo.PersistentAccount;
import rx.Observable;
import rx.functions.Func1;

import java.math.BigDecimal;
import java.util.SortedSet;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.SfsHttpHeaders.X_ACCOUNT_BYTES_USED;
import static org.sfs.util.SfsHttpHeaders.X_ACCOUNT_CONTAINER_COUNT;
import static org.sfs.util.SfsHttpHeaders.X_ACCOUNT_OBJECT_COUNT;
import static org.sfs.util.SfsHttpHeaders.X_ADD_ACCOUNT_META_PREFIX;
import static org.sfs.util.SfsHttpQueryParams.LIMIT;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class HeadAccount implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdmin(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateAccountPath())
                .map(objectPath -> objectPath.accountPath().get())
                .flatMap(new LoadAccount(httpServerRequest.vertxContext()))
                .map(new ValidatePersistentAccountExists())
                .flatMap(new Func1<PersistentAccount, Observable<PersistentAccount>>() {
                    @Override
                    public Observable<PersistentAccount> call(final PersistentAccount persistentAccount) {
                        httpServerRequest.params().set(LIMIT, "0");
                        return just(persistentAccount)
                                .flatMap(new ListContainers(httpServerRequest))
                                .map(containerList -> {
                                    HttpServerResponse httpServerResponse = httpServerRequest.response();
                                    httpServerResponse.putHeader(X_ACCOUNT_OBJECT_COUNT, valueOf(containerList.getObjectCount()));
                                    httpServerResponse.putHeader(X_ACCOUNT_CONTAINER_COUNT, valueOf(containerList.getContainerCount()));
                                    httpServerResponse.putHeader(
                                            X_ACCOUNT_BYTES_USED,
                                            BigDecimal.valueOf(containerList.getBytesUsed())
                                                    .setScale(0, ROUND_HALF_UP)
                                                    .toString()
                                    );
                                    return persistentAccount;
                                });
                    }
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<PersistentAccount>(httpServerRequest) {
                    @Override
                    public void onNext(PersistentAccount account) {
                        HttpServerResponse httpServerResponse = httpServerRequest.response();

                        Metadata metadata = account.getMetadata();

                        for (String key : metadata.keySet()) {
                            SortedSet<String> values = metadata.get(key);
                            if (values != null && !values.isEmpty()) {
                                httpServerResponse.putHeader(format("%s%s", X_ADD_ACCOUNT_META_PREFIX, key), values);
                            }
                        }

                        httpServerResponse.setStatusCode(HTTP_NO_CONTENT);
                    }
                });


    }
}
