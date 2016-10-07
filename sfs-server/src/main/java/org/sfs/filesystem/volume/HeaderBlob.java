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
import static org.sfs.util.NullSafeAscii.equalsIgnoreCase;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_LENGTH;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_POSITION;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_VOLUME;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_VOLUME_PRIMARY;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_VOLUME_REPLICA;

public class HeaderBlob {

    private final String volume;
    private final boolean primary;
    private final boolean replica;
    private final long position;
    private final long length;

    public HeaderBlob(String volume, boolean primary, boolean replica, long position, long length) {
        this.volume = volume;
        this.primary = primary;
        this.replica = replica;
        this.position = position;
        this.length = length;
    }

    public HeaderBlob(HeaderBlob headerBlob) {
        this.volume = headerBlob.volume;
        this.primary = headerBlob.primary;
        this.replica = headerBlob.replica;
        this.position = headerBlob.position;
        this.length = headerBlob.length;
    }

    public HeaderBlob(HttpClientResponse httpClientResponse) {
        MultiMap headers = httpClientResponse.headers();
        this.volume = headers.get(X_CONTENT_VOLUME);
        this.primary = equalsIgnoreCase("true", headers.get(X_CONTENT_VOLUME_PRIMARY));
        this.replica = equalsIgnoreCase("true", headers.get(X_CONTENT_VOLUME_REPLICA));
        this.position = parseLong(headers.get(X_CONTENT_POSITION));
        this.length = parseLong(headers.get(X_CONTENT_LENGTH));
    }

    public HeaderBlob(JsonObject jsonObject) {
        this.volume = jsonObject.getString(X_CONTENT_VOLUME);
        this.primary = jsonObject.getBoolean(X_CONTENT_VOLUME_PRIMARY, false);
        this.replica = jsonObject.getBoolean(X_CONTENT_VOLUME_REPLICA, false);
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

    public boolean isPrimary() {
        return primary;
    }

    public boolean isReplica() {
        return replica;
    }

    public JsonObject toJsonObject() {
        JsonObject jsonObject = new JsonObject()
                .put(X_CONTENT_LENGTH, length)
                .put(X_CONTENT_VOLUME, volume)
                .put(X_CONTENT_VOLUME_PRIMARY, isPrimary())
                .put(X_CONTENT_VOLUME_REPLICA, isReplica())
                .put(X_CONTENT_POSITION, position);
        return jsonObject;
    }

    @Override
    public String toString() {
        return "HeaderBlob{" +
                "length=" + length +
                ", volume='" + volume + '\'' +
                ", primary=" + primary +
                ", replica=" + replica +
                ", position=" + position +
                '}';
    }

    public QuorumIdentity quorumIdentity() {
        return new QuorumIdentity(this);
    }

    public static class QuorumIdentity {

        private final long length;

        public QuorumIdentity(HeaderBlob headerBlob) {
            this.length = headerBlob.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof QuorumIdentity)) return false;

            QuorumIdentity that = (QuorumIdentity) o;

            return length == that.length;

        }

        @Override
        public int hashCode() {
            return (int) (length ^ (length >>> 32));
        }
    }

}
