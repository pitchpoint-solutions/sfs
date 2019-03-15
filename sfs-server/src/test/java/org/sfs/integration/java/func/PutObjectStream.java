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

import com.google.common.collect.ListMultimap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import org.sfs.io.EndableReadStream;
import org.sfs.io.HttpClientRequestEndableWriteStream;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.collect.ArrayListMultimap.create;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;

public class PutObjectStream implements Func1<Void, Observable<HttpClientResponse>> {

    private static final Logger LOGGER = getLogger(PutObjectStream.class);
    private final HttpClient httpClient;
    private final String accountName;
    private final String containerName;
    private final String objectName;
    private final Producer auth;
    private final EndableReadStream<Buffer> data;
    private final ListMultimap<String, String> headers;
    private boolean logRequestBody;
    private boolean chunked;
    private boolean continueHandling;

    public PutObjectStream(HttpClient httpClient, String accountName, String containerName, String objectName, Producer auth, EndableReadStream<Buffer> data, ListMultimap<String, String> headers) {
        this.httpClient = httpClient;
        this.accountName = accountName;
        this.containerName = containerName;
        this.objectName = objectName;
        this.auth = auth;
        this.headers = headers;
        this.data = data;
    }

    public PutObjectStream(HttpClient httpClient, String accountName, String containerName, String objectName, Producer auth, EndableReadStream<Buffer> data) {
        this(httpClient, accountName, containerName, objectName, auth, data, create());
    }

    public PutObjectStream setHeader(String name, String value) {
        headers.removeAll(name);
        headers.put(name, value);
        return this;
    }

    public PutObjectStream setHeader(String name, String value, String... values) {
        headers.removeAll(name);
        headers.put(name, value);
        for (String v : values) {
            headers.put(name, v);
        }
        return this;
    }

    public boolean isChunked() {
        return chunked;
    }

    public PutObjectStream setChunked(boolean chunked) {
        this.chunked = chunked;
        return this;
    }

    public boolean isContinueHandling() {
        return continueHandling;
    }

    public PutObjectStream setContinueHandling(boolean continueHandling) {
        this.continueHandling = continueHandling;
        return this;
    }

    @Override
    public Observable<HttpClientResponse> call(Void aVoid) {
        return auth.toHttpAuthorization()
                .flatMap(s -> {
                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();
                    final HttpClientRequest httpClientRequest =
                            httpClient.put("/openstackswift001/" + accountName + "/" + containerName + "/" + objectName, handler::complete)
                                    .exceptionHandler(handler::fail)
                                    .putHeader(AUTHORIZATION, s);
                    for (String entry : headers.keySet()) {
                        httpClientRequest.putHeader(entry, headers.get(entry));
                    }
                    httpClientRequest.setChunked(isChunked());
                    ObservableFuture<Void> continueHandler = RxHelper.observableFuture();
                    if (isContinueHandling()) {
                        httpClientRequest.putHeader("Expect", "100-Continue");
                        httpClientRequest.continueHandler(event -> continueHandler.complete(null));
                        httpClientRequest.sendHead();
                    } else {
                        continueHandler.complete(null);
                    }

                    return continueHandler.flatMap(aVoid12 -> {
                        Observable<HttpClientResponse> clientResponse = handler;

                        HttpClientRequestEndableWriteStream adapter = new HttpClientRequestEndableWriteStream(httpClientRequest);

                        return combineSinglesDelayError(pump(data, adapter), clientResponse, (aVoid1, httpClientResponse) -> httpClientResponse);
                    });
                });

    }
}
