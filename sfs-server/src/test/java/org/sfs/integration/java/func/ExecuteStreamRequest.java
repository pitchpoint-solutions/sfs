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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.streams.ReadStream;
import org.sfs.io.HttpClientRequestEndableWriteStream;
import org.sfs.io.NoEndEndableWriteStream;
import rx.Observable;

import static org.sfs.io.AsyncIO.pump;

public class ExecuteStreamRequest extends ExecuteRequest {

    private final ReadStream<Buffer> request;
    private final long contentLength;
    private final byte[] contentSha512;

    public ExecuteStreamRequest(HttpClient httpClient, HttpMethod httpMethod, ReadStream<Buffer> request, long length, byte[] contentSha512) {
        super(httpClient, httpMethod);
        this.request = request;
        this.contentLength = length;
        this.contentSha512 = contentSha512;
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
        return pump(request, new NoEndEndableWriteStream(new HttpClientRequestEndableWriteStream(httpClientRequest)));
    }
}
