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

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.ext.unit.TestContext;
import rx.functions.Func1;

import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.net.HttpHeaders.ACCEPT_RANGES;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_MD5;
import static com.google.common.net.HttpHeaders.DATE;
import static com.google.common.net.HttpHeaders.ETAG;
import static com.google.common.net.HttpHeaders.LAST_MODIFIED;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Long.parseLong;
import static java.lang.System.out;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_SHA512;
import static org.sfs.util.SfsHttpHeaders.X_CONTENT_VERSION;
import static org.sfs.util.SfsHttpHeaders.X_SERVER_SIDE_ENCRYPTION;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertNotNull;

public class AssertObjectHeaders implements Func1<HttpClientResponse, HttpClientResponse> {

    private final TestContext context;
    private final long expectedVersion;
    private final boolean serverSideEncryption;
    private final long expectedContentLength;
    private final byte[] dataMd5;
    private final byte[] dataSha512;
    private int assertIndex;

    public AssertObjectHeaders(TestContext context, byte[] data, long expectedVersion, boolean serverSideEncryption, long expectedContentLength, int assertIndex) {
        this(context, expectedVersion, serverSideEncryption, expectedContentLength, md5().hashBytes(data).asBytes(), sha512().hashBytes(data).asBytes(), assertIndex);
    }

    public AssertObjectHeaders(TestContext context, long expectedVersion, boolean serverSideEncryption, long expectedContentLength, byte[] dataMd5, byte[] dataSha512, int assertIndex) {
        this.context = context;
        this.expectedVersion = expectedVersion;
        this.serverSideEncryption = serverSideEncryption;
        this.expectedContentLength = expectedContentLength;
        this.dataMd5 = dataMd5;
        this.dataSha512 = dataSha512;
        this.assertIndex = assertIndex;
    }

    @Override
    public HttpClientResponse call(HttpClientResponse httpClientResponse) {
        out.println("Assert #" + assertIndex);
        MultiMap headers = httpClientResponse.headers();
        String etag = headers.get(ETAG);
        String contentMd5 = headers.get(CONTENT_MD5);
        String contentSha512 = headers.get(X_CONTENT_SHA512);
        String versionId = headers.get(X_CONTENT_VERSION);
        String contentLength = headers.get(CONTENT_LENGTH);
        String acceptRanges = headers.get(ACCEPT_RANGES);
        String lastModified = headers.get(LAST_MODIFIED);
        String date = headers.get(DATE);
        String serverSideEncryption = headers.get(X_SERVER_SIDE_ENCRYPTION);

        assertEquals(context, base16().lowerCase().encode(dataMd5), etag);
        assertEquals(context, base64().encode(dataMd5), contentMd5);
        assertEquals(context, base64().encode(dataSha512), contentSha512);
        assertEquals(context, expectedVersion, parseLong(versionId));
        //VertxAssert.assertEquals(context, expectedContentLength, Long.parseLong(contentLength));
        assertEquals(context, "none", acceptRanges);
        assertNotNull(context, lastModified);
        assertNotNull(context, date);
        assertEquals(context, this.serverSideEncryption, parseBoolean(serverSideEncryption));

        return httpClientResponse;
    }
}
