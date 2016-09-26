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

package org.sfs.integration.java.func;

import com.google.common.collect.ListMultimap;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import org.sfs.rx.MemoizeHandler;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.collect.ArrayListMultimap.create;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;

public class PostAccount implements Func1<Void, Observable<HttpClientResponse>> {

    private static final Logger LOGGER = getLogger(PostAccount.class);
    private final HttpClient httpClient;
    private final String accountName;
    private final Producer auth;
    private final ListMultimap<String, String> headers;

    public PostAccount(HttpClient httpClient, String accountName, Producer auth) {
        this(httpClient, accountName, auth, create());
    }

    public PostAccount(HttpClient httpClient, String accountName, Producer auth, ListMultimap<String, String> headers) {
        this.httpClient = httpClient;
        this.accountName = accountName;
        this.auth = auth;
        this.headers = headers;
    }

    @Override
    public Observable<HttpClientResponse> call(Void aVoid) {
        return auth.toHttpAuthorization()
                .flatMap(new Func1<String, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(String s) {
                        final MemoizeHandler<HttpClientResponse, HttpClientResponse> handler = new MemoizeHandler<>();
                        HttpClientRequest httpClientRequest =
                                httpClient.post("/openstackswift001/" + accountName, handler)
                                        .exceptionHandler(new Handler<Throwable>() {
                                            @Override
                                            public void handle(Throwable event) {
                                                handler.fail(event);
                                            }
                                        })
                                        .setTimeout(10000)
                                        .putHeader(AUTHORIZATION, s);
                        for (String entry : headers.keySet()) {
                            httpClientRequest = httpClientRequest.putHeader(entry, headers.get(entry));
                        }
                        httpClientRequest.end();
                        return Observable.create(handler.subscribe)
                                .single();
                    }
                });

    }
}
