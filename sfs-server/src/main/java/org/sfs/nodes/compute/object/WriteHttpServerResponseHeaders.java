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

package org.sfs.nodes.compute.object;

import com.google.common.base.Optional;
import com.google.common.hash.Hasher;
import io.vertx.core.http.HttpServerResponse;
import org.sfs.SfsRequest;
import org.sfs.metadata.Metadata;
import org.sfs.vo.TransientVersion;
import rx.functions.Func1;

import java.util.Calendar;
import java.util.SortedSet;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.math.LongMath.checkedAdd;
import static com.google.common.net.HttpHeaders.ACCEPT_RANGES;
import static com.google.common.net.HttpHeaders.CONTENT_DISPOSITION;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_MD5;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.DATE;
import static com.google.common.net.HttpHeaders.ETAG;
import static com.google.common.net.HttpHeaders.LAST_MODIFIED;
import static com.google.common.net.MediaType.OCTET_STREAM;
import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.Calendar.getInstance;
import static org.sfs.util.DateFormatter.toRfc1123String;
import static org.sfs.util.SfsHttpHeaders.X_ADD_OBJECT_META_PREFIX;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_SHA512;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_VERSION;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AT;
import static org.sfs.util.SfsHttpHeaders.X_SERVER_SIDE_ENCRYPTION;
import static org.sfs.util.SfsHttpHeaders.X_STATIC_LARGE_OBJECT;

public class WriteHttpServerResponseHeaders implements Func1<Void, Void> {

    private final SfsRequest httpServerRequest;
    private final TransientVersion parentVersion;
    private final Iterable<TransientVersion> largeObjectVersions;

    public WriteHttpServerResponseHeaders(SfsRequest httpServerRequest, TransientVersion parentVersion, Iterable<TransientVersion> largeObjectVersions) {
        this.httpServerRequest = httpServerRequest;
        this.parentVersion = parentVersion;
        this.largeObjectVersions = largeObjectVersions;
    }

    @Override
    public Void call(Void aVoid) {

        HttpServerResponse httpServerResponse = httpServerRequest.response();

        Optional<String> contentDisposition = parentVersion.getContentDisposition();
        Optional<String> contentEncoding = parentVersion.getContentEncoding();
        Optional<Long> deleteAt = parentVersion.getDeleteAt();
        Optional<String> contentType = parentVersion.getContentType();
        Optional<Boolean> serverSideEncryption = parentVersion.getServerSideEncryption();
        Optional<Boolean> oStaticLargeObject = parentVersion.getStaticLargeObject();
        Calendar createTs = parentVersion.getCreateTs();
        Calendar updateTs = parentVersion.getUpdateTs();

        httpServerResponse = httpServerResponse.putHeader(ACCEPT_RANGES, "none");
        httpServerResponse = httpServerResponse.putHeader(LAST_MODIFIED, toRfc1123String(updateTs));
        httpServerResponse = httpServerResponse.putHeader(DATE, toRfc1123String(getInstance()));
        httpServerResponse = httpServerResponse.putHeader(X_CONTENT_VERSION, valueOf(parentVersion.getId()));

        if (contentType.isPresent()) {
            httpServerResponse = httpServerResponse.putHeader(CONTENT_TYPE, contentType.get());
        } else {
            httpServerResponse = httpServerResponse.putHeader(CONTENT_TYPE, OCTET_STREAM.toString());
        }

        if (contentDisposition.isPresent()) {
            httpServerResponse = httpServerResponse.putHeader(CONTENT_DISPOSITION, contentDisposition.get());
        }

        if (contentEncoding.isPresent()) {
            httpServerResponse = httpServerResponse.putHeader(CONTENT_ENCODING, contentEncoding.get());
        }

        if (deleteAt.isPresent()) {
            httpServerResponse = httpServerResponse.putHeader(X_DELETE_AT, valueOf(deleteAt.get()));
        }

        if (serverSideEncryption.isPresent()) {
            httpServerResponse = httpServerResponse.putHeader(X_SERVER_SIDE_ENCRYPTION, serverSideEncryption.get().toString());
        }

        if (oStaticLargeObject.isPresent()) {
            httpServerResponse = httpServerResponse.putHeader(X_STATIC_LARGE_OBJECT, oStaticLargeObject.get().toString());
        }

        if (isEmpty(largeObjectVersions)) {
            byte[] md5 = parentVersion.calculateMd5().get();
            byte[] sha512 = parentVersion.calculateSha512().get();

            httpServerResponse = httpServerResponse.setChunked(true);
            httpServerResponse = httpServerResponse.putHeader(ETAG, base16().lowerCase().encode(md5));
            httpServerResponse = httpServerResponse.putHeader(CONTENT_MD5, base64().encode(md5));
            httpServerResponse = httpServerResponse.putHeader(X_CONTENT_SHA512, base64().encode(sha512));

        } else {

            long contentLengthSum = 0;
            Hasher md5Hasher = md5().newHasher();
            Hasher sha512Hasher = sha512().newHasher();

            int count = 0;
            byte[] firstMd5 = null;
            byte[] firstSha512 = null;

            for (TransientVersion largeObjectVersion : largeObjectVersions) {
                contentLengthSum = checkedAdd(contentLengthSum, largeObjectVersion.calculateLength().get());

                byte[] calculatedMd5 = largeObjectVersion.calculateMd5().get();
                byte[] calculatedSha512 = largeObjectVersion.calculateSha512().get();

                if (count == 0) {
                    firstMd5 = calculatedMd5;
                    firstSha512 = calculatedSha512;
                }

                md5Hasher.putBytes(calculatedMd5);

                sha512Hasher.putBytes(calculatedSha512);

                count++;
            }

            byte[] md5 = count <= 1 ? firstMd5 : md5Hasher.hash().asBytes();
            byte[] sha512 = count <= 1 ? firstSha512 : sha512Hasher.hash().asBytes();


            httpServerResponse = httpServerResponse.setChunked(true);
            httpServerResponse = httpServerResponse.putHeader(ETAG, base16().lowerCase().encode(md5));
            httpServerResponse = httpServerResponse.putHeader(CONTENT_MD5, base64().encode(md5));
            httpServerResponse = httpServerResponse.putHeader(X_CONTENT_SHA512, base64().encode(sha512));
        }


        Metadata metaData = parentVersion.getMetadata();
        for (String key : metaData.keySet()) {
            SortedSet<String> values = metaData.get(key);
            httpServerResponse = httpServerResponse.putHeader(format("%s%s", X_ADD_OBJECT_META_PREFIX, key), values);
        }

        return null;
    }
}
