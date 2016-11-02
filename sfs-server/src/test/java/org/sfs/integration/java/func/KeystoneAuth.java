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

package org.sfs.integration.java.func;

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Charsets.UTF_8;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static rx.Observable.create;
import static rx.Observable.just;

public class KeystoneAuth implements Func1<Void, Observable<HttpClientResponse>> {

    private static final Logger LOGGER = getLogger(KeystoneAuth.class);
    private final HttpClient httpClient;
    private final String username;
    private final String password;
    private String tenantName;

    public KeystoneAuth(HttpClient httpClient, String username, String password) {
        this.httpClient = httpClient;
        this.username = username;
        this.password = password;
    }

    public String getTenantName() {
        return tenantName;
    }

    public KeystoneAuth setTenantName(String tenantName) {
        this.tenantName = tenantName;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public Observable<HttpClientResponse> call(Void aVoid) {
        return just((Void) null)
                .flatMap(new Func1<Void, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(Void aVoid) {
                        ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();
                        HttpClientRequest httpClientRequest =
                                httpClient.post("/v2.0/tokens", handler::complete)
                                        .exceptionHandler(handler::fail)
                                        .setTimeout(10000);

                        JsonObject requestJson =
                                new JsonObject()
                                        .put("auth",
                                                new JsonObject()
                                                        .put("tenantName", tenantName)
                                                        .put("passwordCredentials",
                                                                new JsonObject()
                                                                        .put("username", username)
                                                                        .put("password", password)
                                                        )
                                        );

                        httpClientRequest.end(requestJson.encode(), UTF_8.toString());
                        return handler
                                .single();
                    }
                });

    }
}