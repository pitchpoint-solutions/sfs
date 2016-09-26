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
import com.google.common.escape.Escaper;
import com.google.common.net.MediaType;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import org.sfs.rx.MemoizeHandler;
import rx.Observable;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Joiner.on;
import static com.google.common.collect.ArrayListMultimap.create;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.UrlEscapers.urlFormParameterEscaper;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.util.Collections.addAll;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;

public class GetAccount implements Func1<Void, Observable<HttpClientResponse>> {

    private static final Logger LOGGER = getLogger(GetAccount.class);
    private final HttpClient httpClient;
    private final String accountName;
    private final Producer auth;
    private ListMultimap<String, String> queryParams = create();
    private ListMultimap<String, String> headerParams = create();

    public GetAccount(HttpClient httpClient, String accountName, Producer auth) {
        this.httpClient = httpClient;
        this.accountName = accountName;
        this.auth = auth;
    }

    public GetAccount setMediaTypes(List<MediaType> mediaTypes) {
        headerParams.removeAll(ACCEPT);
        headerParams.putAll(ACCEPT,
                from(mediaTypes)
                        .transform(input -> input.toString()));
        return this;
    }

    public GetAccount setMediaTypes(MediaType mediaType, MediaType... mediaTypes) {
        List<MediaType> types = new ArrayList<>(1 + mediaTypes.length);
        types.add(mediaType);
        addAll(types, mediaTypes);
        return setMediaTypes(types);
    }

    public GetAccount setHeader(String name, String value) {
        headerParams.removeAll(name);
        headerParams.put(name, value);
        return this;
    }

    public GetAccount setHeader(String name, String value, String... values) {
        headerParams.removeAll(name);
        headerParams.put(name, value);
        for (String v : values) {
            headerParams.put(name, v);
        }
        return this;
    }

    public GetAccount setQueryParam(String name, String value) {
        queryParams.removeAll(name);
        queryParams.put(name, value);
        return this;
    }

    public GetAccount setQueryParam(String name, String value, String... values) {
        queryParams.removeAll(name);
        queryParams.put(name, value);
        for (String v : values) {
            queryParams.put(name, v);
        }
        return this;
    }

    @Override
    public Observable<HttpClientResponse> call(Void aVoid) {
        return auth.toHttpAuthorization()
                .flatMap(new Func1<String, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(String s) {

                        final Escaper escaper = urlFormParameterEscaper();

                        Iterable<String> keyValues = from(queryParams.entries())
                                .transform(input -> escaper.escape(input.getKey()) + '=' + escaper.escape(input.getValue()));

                        String query = on('&').join(keyValues);

                        final MemoizeHandler<HttpClientResponse, HttpClientResponse> handler = new MemoizeHandler<>();
                        HttpClientRequest httpClientRequest = httpClient.get("/openstackswift001/" + accountName + (query.length() > 0 ? "?" + query : ""), handler)
                                .exceptionHandler(handler::fail)
                                .setTimeout(10000)
                                .putHeader(AUTHORIZATION, s);

                        for (String entry : headerParams.keySet()) {
                            httpClientRequest = httpClientRequest.putHeader(entry, headerParams.get(entry));
                        }
                        httpClientRequest.end();
                        return Observable.create(handler.subscribe)
                                .single();
                    }
                });

    }
}
