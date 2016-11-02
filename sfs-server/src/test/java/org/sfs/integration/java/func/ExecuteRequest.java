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
import io.vertx.core.http.HttpMethod;
import org.sfs.rx.Holder2;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;

import java.util.Map;

import static com.google.common.collect.ArrayListMultimap.create;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static java.lang.String.valueOf;
import static rx.Observable.error;
import static rx.Observable.just;

public abstract class ExecuteRequest {

    private final HttpClient httpClient;
    private final HttpMethod method;
    private final ListMultimap<String, String> headers = create();
    private boolean chunked;
    private long timeoutMs;
    private boolean signRequest;

    public ExecuteRequest(HttpClient httpClient, HttpMethod httpMethod) {
        this.httpClient = httpClient;
        this.method = httpMethod;
    }

    public ExecuteRequest withHeader(String name, String value) {
        headers.put(name, value);
        return this;
    }

    public boolean isChunked() {
        return chunked;
    }

    public ExecuteRequest setChunked(boolean chunked) {
        this.chunked = chunked;
        return this;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public ExecuteRequest setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public boolean isSignRequest() {
        return signRequest;
    }

    public ExecuteRequest setSignRequest(boolean signRequest) {
        this.signRequest = signRequest;
        return this;
    }

    public abstract long contentLength();

    public abstract byte[] contentSha512();

    public abstract Observable<Void> writeEntity(HttpClientRequest httpClientRequest);

    public Observable<HttpClientResponse> execute(String url) {
        ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();
        HttpClientRequest httpClientRequest =
                httpClient.requestAbs(method, url, httpClientResponse -> {
                    httpClientResponse.pause();
                    handler.complete(httpClientResponse);
                }).setChunked(chunked)
                        .setTimeout(timeoutMs)
                        .exceptionHandler(handler::fail);
        for (Map.Entry<String, String> entry : headers.entries()) {
            httpClientRequest.putHeader(entry.getKey(), entry.getValue());
        }

        long contentLength = contentLength();

        if (!chunked && contentLength >= 0 && !httpClientRequest.headers().contains(CONTENT_LENGTH)) {
            httpClientRequest.putHeader(CONTENT_LENGTH, valueOf(contentLength));
        }
        return writeEntity(httpClientRequest)
                .doOnNext(aVoid -> httpClientRequest.end())
                .flatMap(aVoid -> handler);
    }

    public Observable<Holder2<HttpClientResponse, Buffer>> executeAndBufferResponse(String url) {
        return execute(url)
                .flatMap(httpClientResponse ->
                        just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer())
                                .onErrorResumeNext(throwable -> {
                                    httpClientResponse.resume();
                                    return error(throwable);
                                })
                                .map(buffer -> new Holder2<>(httpClientResponse, buffer)));
    }
}
