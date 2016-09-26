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

package org.sfs.filesystem.volume;

import com.google.common.base.Optional;
import com.google.common.io.BaseEncoding;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.sfs.util.MessageDigestFactory;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.io.BaseEncoding.base64;
import static org.sfs.util.MessageDigestFactory.fromValueIfExists;
import static org.sfs.util.SfsHttpQueryParams.COMPUTED_DIGEST;

public class DigestBlob extends HeaderBlob {

    private final Map<MessageDigestFactory, byte[]> digests;

    public DigestBlob(String volume, boolean primary, boolean replica, long position, long length) {
        super(volume, primary, replica, position, length);
        this.digests = new EnumMap<>(MessageDigestFactory.class);
    }

    public DigestBlob(HeaderBlob headerBlob) {
        super(headerBlob);
        this.digests = new HashMap<>();
    }

    public DigestBlob(HttpClientResponse httpClientResponse) {
        super(httpClientResponse);
        digests = new HashMap<>();
        BaseEncoding baseEncoding = base64();
        MultiMap headers = httpClientResponse.headers();
        for (String headerName : headers.names()) {
            Matcher matcher = COMPUTED_DIGEST.matcher(headerName);
            if (matcher.find()) {
                String digestName = matcher.group(1);
                Optional<MessageDigestFactory> oMessageDigestFactory = fromValueIfExists(digestName);
                if (oMessageDigestFactory.isPresent()) {
                    MessageDigestFactory messageDigestFactory = oMessageDigestFactory.get();
                    withDigest(messageDigestFactory, baseEncoding.decode(headers.get(headerName)));
                }
            }
        }
    }

    public DigestBlob(JsonObject jsonObject) {
        super(jsonObject);
        digests = new HashMap<>();
        JsonArray jsonArray = jsonObject.getJsonArray("X-Computed-Digests");
        if (jsonArray != null) {
            for (Object o : jsonArray) {
                JsonObject jsonDigest = (JsonObject) o;
                String digestName = jsonDigest.getString("name");
                byte[] value = jsonDigest.getBinary("value");
                Optional<MessageDigestFactory> oMessageDigestFactory = fromValueIfExists(digestName);
                if (oMessageDigestFactory.isPresent()) {
                    MessageDigestFactory messageDigestFactory = oMessageDigestFactory.get();
                    withDigest(messageDigestFactory, value);
                }
            }
        }
    }


    public JsonObject toJsonObject() {
        JsonObject jsonObject = super.toJsonObject();

        JsonArray jsonDigests = new JsonArray();

        for (Map.Entry<MessageDigestFactory, byte[]> messageDigest : getDigests().entrySet()) {
            MessageDigestFactory algorithm = messageDigest.getKey();
            byte[] digest = messageDigest.getValue();
            JsonObject jsonDigest =
                    new JsonObject()
                            .put("name", algorithm.getValue())
                            .put("value", digest);
            jsonDigests.add(jsonDigest);
        }

        jsonObject.put("X-Computed-Digests", jsonDigests);

        return jsonObject;
    }

    public DigestBlob withDigest(MessageDigestFactory algorithm, byte[] digest) {
        digests.put(algorithm, digest);
        return this;
    }


    public Map<MessageDigestFactory, byte[]> getDigests() {
        return digests;
    }

    public Optional<byte[]> getDigest(MessageDigestFactory messageDigestFactory) {
        return fromNullable(digests.get(messageDigestFactory));
    }

    public QuorumIdentity quorumIdentity() {
        return new QuorumIdentity(this);
    }

    public static class QuorumIdentity extends HeaderBlob.QuorumIdentity {

        private final Map<MessageDigestFactory, byte[]> digests;

        public QuorumIdentity(DigestBlob digestBlob) {
            super(digestBlob);
            this.digests = digestBlob.getDigests();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof QuorumIdentity)) return false;
            if (!super.equals(o)) return false;

            QuorumIdentity that = (QuorumIdentity) o;

            return digests != null ? digests.equals(that.digests) : that.digests == null;

        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (digests != null ? digests.hashCode() : 0);
            return result;
        }
    }

}
