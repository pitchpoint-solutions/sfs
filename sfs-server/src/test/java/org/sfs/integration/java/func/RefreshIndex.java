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

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import org.sfs.rx.MemoizeHandler;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static rx.Observable.create;

public class RefreshIndex implements Func1<Void, Observable<Void>> {

    private final Producer auth;
    private final HttpClient httpClient;

    public RefreshIndex(HttpClient httpClient, Producer auth) {
        this.httpClient = httpClient;
        this.auth = auth;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        return auth.toHttpAuthorization()
                .flatMap(s -> {
                    final MemoizeHandler<HttpClientResponse, HttpClientResponse> handler = new MemoizeHandler<>();
                    HttpClientRequest httpClientRequest =
                            httpClient.post("/admin/001/refresh_index", handler)
                                    .exceptionHandler(handler::fail)
                                    .putHeader(AUTHORIZATION, s)
                                    .setTimeout(10000);
                    httpClientRequest.end();
                    return create(handler.subscribe)
                            .doOnNext(httpClientResponse -> {
                                checkState(httpClientResponse.statusCode() == HTTP_OK);
                            })
                            .map(new ToVoid<>());
                });

    }
}
