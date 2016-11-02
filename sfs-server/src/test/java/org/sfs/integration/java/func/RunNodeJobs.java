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
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;
import rx.functions.Func1;

import java.nio.file.Path;

import static com.google.common.collect.ArrayListMultimap.create;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;

public class RunNodeJobs implements Func1<Void, Observable<HttpClientResponse>> {

    private static final Logger LOGGER = getLogger(RunNodeJobs.class);
    private final HttpClient httpClient;
    private final Path destinationPath;
    private final Producer auth;
    private final ListMultimap<String, String> headers = create();
    private boolean logRequestBody;

    public RunNodeJobs(HttpClient httpClient, Producer auth, Path destinationPath) {
        this.httpClient = httpClient;
        this.auth = auth;
        this.destinationPath = destinationPath;
    }

    public RunNodeJobs setHeader(String name, String value) {
        headers.removeAll(name);
        headers.put(name, value);
        return this;
    }

    public RunNodeJobs setHeader(String name, String value, String... values) {
        headers.removeAll(name);
        headers.put(name, value);
        for (String v : values) {
            headers.put(name, v);
        }
        return this;
    }

    @Override
    public Observable<HttpClientResponse> call(Void aVoid) {
        return auth.toHttpAuthorization()
                .flatMap(s -> {
                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();
                    HttpClientRequest httpClientRequest =
                            httpClient.post("/admin/001/run_jobs", handler::complete)
                                    .exceptionHandler(handler::fail)
                                    .setTimeout(1000)
                                    .putHeader(AUTHORIZATION, s);
                    for (String entry : headers.keySet()) {
                        httpClientRequest = httpClientRequest.putHeader(entry, headers.get(entry));
                    }
                    httpClientRequest.end();
                    return handler
                            .single();
                });

    }
}
