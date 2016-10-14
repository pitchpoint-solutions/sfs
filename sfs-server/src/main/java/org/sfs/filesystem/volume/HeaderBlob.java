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

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;

import static java.lang.Long.parseLong;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_LENGTH;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_POSITION;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_VOLUME;

public class HeaderBlob {

    private final String volume;
    private final long position;
    private final long length;

    public HeaderBlob(String volume, long position, long length) {
        this.volume = volume;
        this.position = position;
        this.length = length;
    }

    public HeaderBlob(HeaderBlob headerBlob) {
        this.volume = headerBlob.volume;
        this.position = headerBlob.position;
        this.length = headerBlob.length;
    }

    public HeaderBlob(HttpClientResponse httpClientResponse) {
        MultiMap headers = httpClientResponse.headers();
        this.volume = headers.get(X_CONTENT_VOLUME);
        this.position = parseLong(headers.get(X_CONTENT_POSITION));
        this.length = parseLong(headers.get(X_CONTENT_LENGTH));
    }

    public HeaderBlob(JsonObject jsonObject) {
        this.volume = jsonObject.getString(X_CONTENT_VOLUME);
        this.position = jsonObject.getLong(X_CONTENT_POSITION);
        this.length = jsonObject.getLong(X_CONTENT_LENGTH);
    }


    public long getLength() {
        return length;
    }

    public String getVolume() {
        return volume;
    }

    public long getPosition() {
        return position;
    }

    public JsonObject toJsonObject() {
        JsonObject jsonObject = new JsonObject()
                .put(X_CONTENT_LENGTH, length)
                .put(X_CONTENT_VOLUME, volume)
                .put(X_CONTENT_POSITION, position);
        return jsonObject;
    }

    @Override
    public String toString() {
        return "HeaderBlob{" +
                "length=" + length +
                ", volume='" + volume + '\'' +
                ", position=" + position +
                '}';
    }
}
