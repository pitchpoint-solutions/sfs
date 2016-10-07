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

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import org.sfs.nodes.HttpClientResponseException;
import org.sfs.rx.ResultMemoizeHandler;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static rx.Observable.create;

public class WaitForCluster implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(WaitForCluster.class);
    private Vertx vertx;
    private final HttpClient httpClient;

    public WaitForCluster(Vertx vertx, HttpClient httpClient) {
        this.vertx = vertx;
        this.httpClient = httpClient;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        final ResultMemoizeHandler<Void> handler = new ResultMemoizeHandler<>();
        vertx.setPeriodic(100, timerId -> {
            final ResultMemoizeHandler<HttpClientResponse> internalHandler = new ResultMemoizeHandler<>();
            HttpClientRequest httpClientRequest =
                    httpClient.get("/admin/001/is_online", internalHandler)
                            .exceptionHandler(handler::fail)
                            .setTimeout(10000);
            httpClientRequest.end();
            create(internalHandler.subscribe)
                    .subscribe(new Subscriber<HttpClientResponse>() {

                        HttpClientResponse result;

                        @Override
                        public void onCompleted() {
                            if (HTTP_OK == result.statusCode()) {
                                vertx.cancelTimer(timerId);
                                handler.complete(null);
                            } else if (HTTP_NO_CONTENT == result.statusCode()) {

                            } else {
                                onError(new HttpClientResponseException(result, buffer()));
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            vertx.cancelTimer(timerId);
                            handler.fail(e);
                        }

                        @Override
                        public void onNext(HttpClientResponse httpClientResponse) {
                            result = httpClientResponse;
                        }
                    });
        });
        return create(handler.subscribe);
    }
}