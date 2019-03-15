/*
 * Copyright 2019 The Simple File Server Authors
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
import io.vertx.core.logging.Logger;
import org.apache.commons.lang3.StringUtils;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class GetHealth implements Func1<Void, Observable<HttpClientResponse>> {

    private static final Logger LOGGER = getLogger(GetHealth.class);
    private final HttpClient httpClient;
    private String contextRoot;


    public GetHealth(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public String getContextRoot() {
        return contextRoot;
    }

    public GetHealth setContextRoot(String contextRoot) {
        this.contextRoot = contextRoot;
        return this;
    }

    @Override
    public Observable<HttpClientResponse> call(Void aVoid) {
        StringBuilder urlBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(contextRoot)) {
            urlBuilder = urlBuilder.append("/" + contextRoot);
        }
        urlBuilder = urlBuilder.append("/admin/001/healthcheck");

        ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();
        HttpClientRequest httpClientRequest =
                httpClient.get(urlBuilder.toString(), handler::complete)
                        .exceptionHandler(handler::fail)
                        .setTimeout(5000);
        httpClientRequest.end();
        return handler
                .single();

    }
}
