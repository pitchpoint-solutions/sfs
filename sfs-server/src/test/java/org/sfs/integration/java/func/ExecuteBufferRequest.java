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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import rx.Observable;

import static org.sfs.util.MessageDigestFactory.SHA512;
import static rx.Observable.just;

public class ExecuteBufferRequest extends ExecuteRequest {

    private final Buffer buffer;
    private final long contentLength;
    private final byte[] contentSha512;

    public ExecuteBufferRequest(HttpClient httpClient, HttpMethod httpMethod, Buffer request) {
        super(httpClient, httpMethod);
        this.buffer = request;
        this.contentLength = request.length();
        this.contentSha512 = SHA512.instance().digest(request.getBytes());
    }

    @Override
    public long contentLength() {
        return contentLength;
    }

    @Override
    public byte[] contentSha512() {
        return contentSha512;
    }

    @Override
    public Observable<Void> writeEntity(HttpClientRequest httpClientRequest) {
        return just((Void) null)
                .doOnNext(aVoid -> httpClientRequest.write(buffer));
    }
}
