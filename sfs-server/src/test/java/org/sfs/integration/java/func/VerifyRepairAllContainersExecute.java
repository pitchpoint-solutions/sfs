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

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ListMultimap;
import com.google.common.escape.Escaper;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.logging.Logger;
import org.sfs.integration.java.help.AuthorizationFactory;
import org.sfs.jobs.Jobs;
import org.sfs.rx.Defer;
import org.sfs.rx.HttpClientKeepAliveResponseBodyBuffer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;
import rx.functions.Func1;

import java.util.concurrent.TimeUnit;

import static com.google.common.net.UrlEscapers.urlFormParameterEscaper;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class VerifyRepairAllContainersExecute implements Func1<Void, Observable<HttpClientResponseAndBuffer>> {

    private static final Logger LOGGER = getLogger(ContainerExport.class);
    private final HttpClient httpClient;
    private final AuthorizationFactory.Producer auth;
    private ListMultimap<String, String> queryParams = ArrayListMultimap.create();
    private ListMultimap<String, String> headerParams = ArrayListMultimap.create();

    public VerifyRepairAllContainersExecute(HttpClient httpClient, AuthorizationFactory.Producer auth) {
        this.httpClient = httpClient;
        this.auth = auth;
    }

    public VerifyRepairAllContainersExecute setHeader(String name, String value) {
        headerParams.removeAll(name);
        headerParams.put(name, value);
        return this;
    }

    public VerifyRepairAllContainersExecute setHeader(String name, String value, String... values) {
        headerParams.removeAll(name);
        headerParams.put(name, value);
        for (String v : values) {
            headerParams.put(name, v);
        }
        return this;
    }

    public VerifyRepairAllContainersExecute setQueryParam(String name, String value) {
        queryParams.removeAll(name);
        queryParams.put(name, value);
        return this;
    }

    public VerifyRepairAllContainersExecute setQueryParam(String name, String value, String... values) {
        queryParams.removeAll(name);
        queryParams.put(name, value);
        for (String v : values) {
            queryParams.put(name, v);
        }
        return this;
    }

    @Override
    public Observable<HttpClientResponseAndBuffer> call(Void aVoid) {
        return auth.toHttpAuthorization()
                .flatMap(s -> {
                    final Escaper escaper = urlFormParameterEscaper();

                    Iterable<String> keyValues = FluentIterable.from(queryParams.entries())
                            .transform(input -> escaper.escape(input.getKey()) + '=' + escaper.escape(input.getValue()));

                    String query = Joiner.on('&').join(keyValues);

                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();
                    HttpClientRequest httpClientRequest =
                            httpClient.post("/verify_repair_containers" + (query.length() > 0 ? "?" + query : ""), handler::complete)
                                    .exceptionHandler(handler::fail)
                                    .setTimeout(20000)
                                    .putHeader(Jobs.Parameters.TIMEOUT, String.valueOf(TimeUnit.MINUTES.toMillis(1)))
                                    .putHeader(HttpHeaders.AUTHORIZATION, s);
                    for (String entry : headerParams.keySet()) {
                        httpClientRequest = httpClientRequest.putHeader(entry, headerParams.get(entry));
                    }
                    httpClientRequest.end();
                    return handler
                            .flatMap(httpClientResponse ->
                                    Defer.just(httpClientResponse)
                                            .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                                            .map(buffer -> new HttpClientResponseAndBuffer(httpClientResponse, buffer)))
                            .single();
                });
    }
}
