/*
 * Copyright 2018 The Simple File Server Authors
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

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.exceptions.CompositeException;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class WaitForHealthCheck implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(WaitForHealthCheck.class);
    private HttpClient httpClient;
    private Vertx vertx;

    public WaitForHealthCheck(HttpClient httpClient, Vertx vertx) {
        this.httpClient = httpClient;
        this.vertx = vertx;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        Func0<Observable<Void>> func0 = () -> {
            StringBuilder urlBuilder = new StringBuilder();
            urlBuilder = urlBuilder.append("/admin/001/healthcheck");
            ObservableFuture<HttpClientResponse> httpHandler = RxHelper.observableFuture();
            HttpClientRequest httpClientRequest =
                    httpClient.get(urlBuilder.toString(), httpHandler::complete)
                            .exceptionHandler(httpHandler::fail)
                            .setTimeout(5000);
            httpClientRequest.end();
            return httpHandler.flatMap(new HttpClientResponseBodyBuffer())
                    .map(new ToVoid<>());
        };
        List<Throwable> errors = new ArrayList<>();
        return Observable.just((Void) null)
                .flatMap(aVoid1 -> RxHelper.onErrorResumeNext(20, func0))
                .onErrorResumeNext(throwable -> {
                    LOGGER.warn("Handling connect failure to health check", throwable);
                    errors.add(throwable);
                    return Observable.error(new CompositeException(errors));
                });
    }
}