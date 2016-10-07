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
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;

public class PutObject implements Func1<Void, Observable<HttpClientResponse>> {

    private static final Logger LOGGER = getLogger(PutObject.class);
    private final HttpClient httpClient;
    private final String accountName;
    private final String containerName;
    private final String objectName;
    private final Producer auth;
    private final byte[] data;
    private final ListMultimap<String, String> headers;
    private boolean logRequestBody;
    private boolean chunked;

    public PutObject(HttpClient httpClient, String accountName, String containerName, String objectName, Producer auth, byte[] data, ListMultimap<String, String> headers) {
        this.httpClient = httpClient;
        this.accountName = accountName;
        this.containerName = containerName;
        this.objectName = objectName;
        this.auth = auth;
        this.headers = headers;
        this.data = data;
    }

    public PutObject(HttpClient httpClient, String accountName, String containerName, String objectName, Producer auth, byte[] data) {
        this(httpClient, accountName, containerName, objectName, auth, data, create());
    }

    public PutObject setHeader(String name, String value) {
        headers.removeAll(name);
        headers.put(name, value);
        return this;
    }

    public PutObject setHeader(String name, String value, String... values) {
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

    public PutObject setChunked(boolean chunked) {
        this.chunked = chunked;
        return this;
    }

    @Override
    public Observable<HttpClientResponse> call(Void aVoid) {
        return auth.toHttpAuthorization()
                .flatMap(new Func1<String, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(String s) {
                        final MemoizeHandler<HttpClientResponse, HttpClientResponse> handler = new MemoizeHandler<>();
                        HttpClientRequest httpClientRequest =
                                httpClient.put("/openstackswift001/" + accountName + "/" + containerName + "/" + objectName, handler)
                                        .exceptionHandler(new Handler<Throwable>() {
                                            @Override
                                            public void handle(Throwable event) {
                                                handler.fail(event);
                                            }
                                        })
                                        .setTimeout(20000)
                                        .putHeader(AUTHORIZATION, s);
                        for (String entry : headers.keySet()) {
                            httpClientRequest = httpClientRequest.putHeader(entry, headers.get(entry));
                        }
                        httpClientRequest.setChunked(isChunked());
                        httpClientRequest.end(buffer(data));
                        return Observable.create(handler.subscribe)
                                .single();
                    }
                });

    }
}
