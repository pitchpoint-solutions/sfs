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

package org.sfs.rx;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Charsets.UTF_8;
import static io.vertx.core.buffer.Buffer.buffer;
import static rx.Observable.just;

public class HttpClientKeepAliveResponseBodyBuffer implements Func1<HttpClientResponse, Observable<Buffer>> {

    @Override
    public Observable<Buffer> call(HttpClientResponse httpClientResponse) {
        return just(httpClientResponse)
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(buffer -> buffer(buffer.toString(UTF_8.toString()).trim(), UTF_8.toString()));
    }
}
