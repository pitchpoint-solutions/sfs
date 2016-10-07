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

package org.sfs.filesystem.volume;

import io.vertx.core.http.HttpClientResponse;
import org.sfs.io.BufferEndableWriteStream;
import rx.Observable;

import static java.lang.Long.parseLong;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_OFFSET;

public abstract class ReadStreamBlob extends HeaderBlob {

    private final long offset;

    public ReadStreamBlob(String volume, boolean primary, boolean replica, long position, long offset, long length) {
        super(volume, primary, replica, position, length);
        this.offset = offset;
    }

    public ReadStreamBlob(HttpClientResponse httpClientResponse) {
        super(httpClientResponse);
        this.offset = parseLong(httpClientResponse.headers().get(X_CONTENT_OFFSET));
    }

    @Override
    public String toString() {
        return "ReadStreamBlob{" +
                "offset=" + offset +
                "} " + super.toString();
    }

    public long getOffset() {
        return offset;
    }

    public abstract Observable<Void> produce(BufferEndableWriteStream bufferStreamConsumer);
}
