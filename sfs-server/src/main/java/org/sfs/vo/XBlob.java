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

package org.sfs.vo;

import com.google.common.base.Optional;
import com.google.common.primitives.Longs;
import io.vertx.core.MultiMap;
import org.sfs.SfsRequest;
import org.sfs.util.MessageDigestFactory;

import java.util.List;
import java.util.regex.Matcher;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.primitives.Ints.tryParse;
import static java.util.Collections.emptyList;
import static org.sfs.util.MessageDigestFactory.fromValueIfExists;
import static org.sfs.util.SfsHttpQueryParams.COMPUTED_DIGEST;
import static org.sfs.util.SfsHttpQueryParams.POSITION;
import static org.sfs.util.SfsHttpQueryParams.VERSION;
import static org.sfs.util.SfsHttpQueryParams.VOLUME;

public class XBlob {

    private Integer volume;
    private Long position;
    private Long length;
    private byte[] sha512;
    private byte[] version;
    private Long ttl;
    private List<MessageDigestFactory> messageDigests;

    public Optional<Integer> getVolume() {
        return fromNullable(volume);
    }

    public XBlob setVolume(Integer volume) {
        this.volume = volume;
        return this;
    }

    public Optional<Long> getPosition() {
        return fromNullable(position);
    }

    public XBlob setPosition(Long position) {
        this.position = position;
        return this;
    }

    public Optional<byte[]> getSha512() {
        return fromNullable(sha512);
    }

    public XBlob setSha512(byte[] sha512) {
        this.sha512 = sha512;
        return this;
    }

    public Optional<Long> getLength() {
        return fromNullable(length);
    }

    public XBlob setLength(Long length) {
        this.length = length;
        return this;
    }

    public Optional<byte[]> getVersion() {
        return fromNullable(version);
    }

    public XBlob setVersion(byte[] version) {
        this.version = version;
        return this;
    }

    public Optional<Long> getTtl() {
        return fromNullable(ttl);
    }

    public XBlob setTtl(Long ttl) {
        this.ttl = ttl;
        return this;
    }

    public List<MessageDigestFactory> getMessageDigests() {
        if (messageDigests == null) {
            return emptyList();
        }
        return messageDigests;
    }

    public XBlob merge(SfsRequest httpServerRequest) {
        MultiMap queryParams = httpServerRequest.params();
        MultiMap headers = httpServerRequest.headers();

        if (queryParams.contains(VOLUME)) {
            volume = tryParse(queryParams.get(VOLUME));
        }
        if (queryParams.contains(POSITION)) {
            position = Longs.tryParse(queryParams.get(POSITION));
        }
        if (queryParams.contains(VERSION)) {
            version = base64().decode(queryParams.get(VERSION));
        }
        if (headers.contains(CONTENT_LENGTH)) {
            length = Longs.tryParse(headers.get(CONTENT_LENGTH));
        }

        for (String queryParam : queryParams.names()) {
            Matcher matcher = COMPUTED_DIGEST.matcher(queryParam);
            if (matcher.matches()) {
                MessageDigestFactory digest = fromValueIfExists(matcher.group(1)).get();
                messageDigests.add(digest);
            }
        }

        return this;
    }
}
