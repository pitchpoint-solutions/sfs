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

package org.sfs.integration.java.test.object;

import com.google.common.base.Optional;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.IndexRefresh;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.AssertObjectData;
import org.sfs.integration.java.func.AssertObjectHeaders;
import org.sfs.integration.java.func.DeleteObject;
import org.sfs.integration.java.func.GetObject;
import org.sfs.integration.java.func.HeadObject;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PostContainer;
import org.sfs.integration.java.func.PostObject;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.PutObjectStream;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.io.CountingEndableWriteStream;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.jobs.Jobs;
import org.sfs.rx.BufferToJsonObject;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.ByteStreams.nullOutputStream;
import static com.google.common.io.Files.hash;
import static com.google.common.net.HttpHeaders.CONTENT_DISPOSITION;
import static com.google.common.net.HttpHeaders.CONTENT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.ETAG;
import static java.lang.String.valueOf;
import static java.lang.System.arraycopy;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.out;
import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.Files.size;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.getInstance;
import static org.sfs.elasticsearch.object.MaintainObjectsForNode.CONSISTENCY_THRESHOLD;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.KnownMetadataKeys.X_MAX_OBJECT_REVISIONS;
import static org.sfs.util.MessageDigestFactory.MD5;
import static org.sfs.util.MessageDigestFactory.SHA512;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META_PREFIX;
import static org.sfs.util.SfsHttpHeaders.X_ADD_OBJECT_META_PREFIX;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AT;
import static org.sfs.util.SfsHttpHeaders.X_OBJECT_MANIFEST;
import static org.sfs.util.SfsHttpHeaders.X_SERVER_SIDE_ENCRYPTION;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertFalse;
import static org.sfs.util.VertxAssert.assertNull;
import static org.sfs.util.VertxAssert.assertTrue;
import static org.sfs.vo.ObjectPath.fromPaths;
import static rx.Observable.just;

public class CreateUpdateDeleteObjectTest extends BaseTestVerticle {

    private final String accountName = "testaccount";
    private final String containerName = "testcontainer";
    private final String objectName = "testobject";

    private Producer authAdmin = httpBasic("admin", "admin");
    private Producer authNonAdmin = httpBasic("user", "user");

    protected Observable<Void> prepareContainer(TestContext context) {

        return just((Void) null)
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().getClusterInfo().forceRefresh(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().getClusterInfo().forceRefresh(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new PostAccount(HTTP_CLIENT, accountName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(3)))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>());
    }

    @Test
    public void testPurgeExpired(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final long currentTimeInMillis = currentTimeMillis();

        final Jobs masterTasksManager = VERTX_CONTEXT.verticle().jobs();

        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0)
                        .setHeader(X_DELETE_AT, valueOf(currentTimeInMillis)))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new Func1<Void, Observable<GetResponse>>() {
                    @Override
                    public Observable<GetResponse> call(Void aVoid) {
                        Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                                .map(new Func1<Optional<GetResponse>, GetResponse>() {
                                    @Override
                                    public GetResponse call(Optional<GetResponse> getResponseOptional) {
                                        return getResponseOptional.get();
                                    }
                                });
                    }
                })
                .map(new Func1<GetResponse, GetResponse>() {
                    @Override
                    public GetResponse call(GetResponse getResponse) {
                        assertTrue(context, getResponse.isExists());
                        return getResponse;
                    }
                })
                .flatMap(new Func1<GetResponse, Observable<IndexResponse>>() {
                    @Override
                    public Observable<IndexResponse> call(GetResponse getResponse) {
                        Calendar afterConsistencyWindow = getInstance();
                        afterConsistencyWindow.add(MILLISECOND, -(CONSISTENCY_THRESHOLD * 2));

                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        jsonObject.put("update_ts", toDateTimeString(afterConsistencyWindow));

                        Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                        Client client = elasticsearch.get();
                        IndexRequestBuilder request =
                                client.prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType())
                                        .setId(getResponse.getId())
                                        .setVersion(getResponse.getVersion())
                                        .setSource(jsonObject.encode());

                        return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    }
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new Func1<Void, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(Void aVoid) {
                        return masterTasksManager.run(VERTX_CONTEXT);
                    }
                })
                .flatMap(new Func1<Void, Observable<GetResponse>>() {
                    @Override
                    public Observable<GetResponse> call(Void aVoid) {
                        Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    }
                })
                .map(getResponse -> {
                    assertFalse(context, getResponse.isExists());
                    return getResponse;
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test(timeout = 1200000)
    public void testEncryptedDynamicLargeLargeUpload(TestContext context) throws IOException {

        byte[] data = new byte[256];
        getCurrentInstance().nextBytes(data);
        int dataSize = 256 * 1024;

        Path tempFile1 = createTempFile(tmpDir, "", "");
        Path tempFile2 = createTempFile(tmpDir, "", "");
        Path tempFile3 = createTempFile(tmpDir, "", "");

        int bytesWritten = 0;


        try (OutputStream out1 = newOutputStream(tempFile1);
             OutputStream out2 = newOutputStream(tempFile2);
             OutputStream out3 = newOutputStream(tempFile3)) {
            while (bytesWritten < dataSize) {
                out1.write(data);
                out2.write(data);
                out3.write(data);
                bytesWritten += data.length;
            }
        }

        final long size1 = size(tempFile1);
        final long size2 = size(tempFile2);
        final long size3 = size(tempFile3);

        final byte[] md51 = hash(tempFile1.toFile(), md5()).asBytes();
        final byte[] md52 = hash(tempFile2.toFile(), md5()).asBytes();
        final byte[] md53 = hash(tempFile3.toFile(), md5()).asBytes();

        final byte[] sha5121 = hash(tempFile1.toFile(), sha512()).asBytes();
        final byte[] sha5122 = hash(tempFile2.toFile(), sha512()).asBytes();
        final byte[] sha5123 = hash(tempFile3.toFile(), sha512()).asBytes();

        final byte[] contactedMd5 =
                md5().newHasher()
                        .putBytes(md51)
                        .putBytes(md52)
                        .putBytes(md53)
                        .hash()
                        .asBytes();

        final byte[] contactedSha512 =
                sha512().newHasher()
                        .putBytes(sha5121)
                        .putBytes(sha5122)
                        .putBytes(sha5123)
                        .hash()
                        .asBytes();

        MessageDigest sha512Digest = SHA512.instance();
        MessageDigest md5Digest = MD5.instance();
        DigestOutputStream digestOutputStream = new DigestOutputStream(new DigestOutputStream(nullOutputStream(), sha512Digest), md5Digest);

        copy(tempFile1, digestOutputStream);
        copy(tempFile2, digestOutputStream);
        copy(tempFile3, digestOutputStream);

        digestOutputStream.close();

        final byte[] expectedStreamMd5 = md5Digest.digest();
        final byte[] expectedStreamSha512 = sha512Digest.digest();

        final AsyncFile bigFile1 = VERTX.fileSystem().openBlocking(tempFile1.toString(), new OpenOptions());
        final AsyncFile bigFile2 = VERTX.fileSystem().openBlocking(tempFile2.toString(), new OpenOptions());
        final AsyncFile bigFile3 = VERTX.fileSystem().openBlocking(tempFile3.toString(), new OpenOptions());

        Async async = context.async();
        prepareContainer(context)
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName + "/segments/0", authNonAdmin, bigFile1)
                        .setHeader(CONTENT_LENGTH, valueOf(size1))
                        .setHeader(ETAG, base16().lowerCase().encode(md51))
                        .setHeader(X_SERVER_SIDE_ENCRYPTION, "true"))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName + "/segments/1", authNonAdmin, bigFile2)
                        .setHeader(CONTENT_LENGTH, valueOf(size2))
                        .setHeader(ETAG, base16().lowerCase().encode(md52))
                        .setHeader(X_SERVER_SIDE_ENCRYPTION, "true"))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName + "/segments/3", authNonAdmin, bigFile3)
                        .setHeader(CONTENT_LENGTH, valueOf(size3))
                        .setHeader(ETAG, base16().lowerCase().encode(md53))
                        .setHeader(X_SERVER_SIDE_ENCRYPTION, "true"))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, new byte[]{})
                        .setHeader(X_OBJECT_MANIFEST, containerName + "/" + objectName + "/segments/"))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, 0, false, size1 + size2 + size3, contactedMd5, contactedSha512, 0))
                .flatMap(new Func1<HttpClientResponse, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(final HttpClientResponse httpClientResponse) {
                        final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512, MD5);
                        final CountingEndableWriteStream countingWriteStream = new CountingEndableWriteStream(digestWriteStream);
                        return pump(httpClientResponse, countingWriteStream)
                                .map(new Func1<Void, HttpClientResponse>() {
                                    @Override
                                    public HttpClientResponse call(Void aVoid) {
                                        assertEquals(context, size1 + size2 + size3, countingWriteStream.count());
                                        assertArrayEquals(context, expectedStreamMd5, digestWriteStream.getDigest(MD5).get());
                                        assertArrayEquals(context, expectedStreamSha512, digestWriteStream.getDigest(SHA512).get());
                                        return httpClientResponse;
                                    }
                                });
                    }
                })
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }


    @Test(timeout = 1200000)
    public void testDynamicLargeLargeUpload(TestContext context) throws IOException {

        byte[] data = new byte[256];
        getCurrentInstance().nextBytes(data);
        int dataSize = 256 * 1024;

        Path tempFile1 = createTempFile(tmpDir, "", "");
        Path tempFile2 = createTempFile(tmpDir, "", "");
        Path tempFile3 = createTempFile(tmpDir, "", "");

        int bytesWritten = 0;

        try (OutputStream out1 = newOutputStream(tempFile1);
             OutputStream out2 = newOutputStream(tempFile2);
             OutputStream out3 = newOutputStream(tempFile3)) {
            while (bytesWritten < dataSize) {
                out1.write(data);
                out2.write(data);
                out3.write(data);
                bytesWritten += data.length;
            }
        }

        final long size1 = size(tempFile1);
        final long size2 = size(tempFile2);
        final long size3 = size(tempFile3);

        final byte[] md51 = hash(tempFile1.toFile(), md5()).asBytes();
        final byte[] md52 = hash(tempFile2.toFile(), md5()).asBytes();
        final byte[] md53 = hash(tempFile3.toFile(), md5()).asBytes();

        final byte[] sha5121 = hash(tempFile1.toFile(), sha512()).asBytes();
        final byte[] sha5122 = hash(tempFile2.toFile(), sha512()).asBytes();
        final byte[] sha5123 = hash(tempFile3.toFile(), sha512()).asBytes();

        final byte[] contactedMd5 =
                md5().newHasher()
                        .putBytes(md51)
                        .putBytes(md52)
                        .putBytes(md53)
                        .hash()
                        .asBytes();

        final byte[] contactedSha512 =
                sha512().newHasher()
                        .putBytes(sha5121)
                        .putBytes(sha5122)
                        .putBytes(sha5123)
                        .hash()
                        .asBytes();

        MessageDigest sha512Digest = SHA512.instance();
        MessageDigest md5Digest = MD5.instance();
        DigestOutputStream digestOutputStream = new DigestOutputStream(new DigestOutputStream(nullOutputStream(), sha512Digest), md5Digest);

        copy(tempFile1, digestOutputStream);
        copy(tempFile2, digestOutputStream);
        copy(tempFile3, digestOutputStream);

        digestOutputStream.close();

        final byte[] expectedStreamMd5 = md5Digest.digest();
        final byte[] expectedStreamSha512 = sha512Digest.digest();

        final AsyncFile bigFile1 = VERTX.fileSystem().openBlocking(tempFile1.toString(), new OpenOptions());
        final AsyncFile bigFile2 = VERTX.fileSystem().openBlocking(tempFile2.toString(), new OpenOptions());
        final AsyncFile bigFile3 = VERTX.fileSystem().openBlocking(tempFile3.toString(), new OpenOptions());

        Async async = context.async();
        prepareContainer(context)
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName + "/segments/0", authNonAdmin, bigFile1)
                        .setHeader(CONTENT_LENGTH, valueOf(size1))
                        .setHeader(ETAG, base16().lowerCase().encode(md51)))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName + "/segments/1", authNonAdmin, bigFile2)
                        .setHeader(CONTENT_LENGTH, valueOf(size2))
                        .setHeader(ETAG, base16().lowerCase().encode(md52)))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName + "/segments/3", authNonAdmin, bigFile3)
                        .setHeader(CONTENT_LENGTH, valueOf(size3))
                        .setHeader(ETAG, base16().lowerCase().encode(md53)))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, new byte[]{})
                        .setHeader(X_OBJECT_MANIFEST, containerName + "/" + objectName + "/segments/"))
                .map(new ToVoid<HttpClientResponse>())
                // wait for the index to refresh
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                    @Override
                    public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                        return httpClientResponse.pause();
                    }
                })
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, 0, false, size1 + size2 + size3, contactedMd5, contactedSha512, 0))
                .flatMap(new Func1<HttpClientResponse, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(final HttpClientResponse httpClientResponse) {
                        final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512, MD5);
                        final CountingEndableWriteStream countingWriteStream = new CountingEndableWriteStream(digestWriteStream);
                        return pump(httpClientResponse, countingWriteStream)
                                .map(new Func1<Void, HttpClientResponse>() {
                                    @Override
                                    public HttpClientResponse call(Void aVoid) {
                                        assertEquals(context, size1 + size2 + size3, countingWriteStream.count());
                                        assertArrayEquals(context, expectedStreamMd5, digestWriteStream.getDigest(MD5).get());
                                        assertArrayEquals(context, expectedStreamSha512, digestWriteStream.getDigest(SHA512).get());
                                        return httpClientResponse;
                                    }
                                });
                    }
                })
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testLargeUpload(TestContext context) throws IOException {

        byte[] data = new byte[256];
        getCurrentInstance().nextBytes(data);
        int dataSize = 256 * 1024;

        Path tempFile = createTempFile(tmpDir, "", "");

        int bytesWritten = 0;
        try (OutputStream out = newOutputStream(tempFile)) {
            while (bytesWritten < dataSize) {
                out.write(data);
                bytesWritten += data.length;
            }
        }

        long size = size(tempFile);
        final byte[] md5 = hash(tempFile.toFile(), md5()).asBytes();
        final byte[] sha512 = hash(tempFile.toFile(), sha512()).asBytes();
        final AsyncFile bigFile = VERTX.fileSystem().openBlocking(tempFile.toString(), new OpenOptions());

        Async async = context.async();
        prepareContainer(context)

                // put an object then get/head the object
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, bigFile)
                        .setHeader(CONTENT_LENGTH, valueOf(size))
                        .setHeader(ETAG, base16().lowerCase().encode(md5)))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, 0, false, 0, md5, sha512, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, 0, false, size, md5, sha512, 1))
                .flatMap(new Func1<HttpClientResponse, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(final HttpClientResponse httpClientResponse) {
                        final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512, MD5);
                        return pump(httpClientResponse, digestWriteStream)
                                .map(new Func1<Void, HttpClientResponse>() {
                                    @Override
                                    public HttpClientResponse call(Void aVoid) {
                                        assertArrayEquals(context, md5, digestWriteStream.getDigest(MD5).get());
                                        assertArrayEquals(context, sha512, digestWriteStream.getDigest(SHA512).get());
                                        return httpClientResponse;
                                    }
                                });
                    }
                })
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testEncryptedLargeUpload(TestContext context) throws IOException {

        byte[] data = new byte[256];
        getCurrentInstance().nextBytes(data);
        int dataSize = 256 * 1024;

        Path tempFile = createTempFile(tmpDir, "", "");

        int bytesWritten = 0;
        try (OutputStream out = newOutputStream(tempFile)) {
            while (bytesWritten < dataSize) {
                out.write(data);
                bytesWritten += data.length;
            }
        }

        long size = size(tempFile);
        final byte[] md5 = hash(tempFile.toFile(), md5()).asBytes();
        final byte[] sha512 = hash(tempFile.toFile(), sha512()).asBytes();
        final AsyncFile bigFile = VERTX.fileSystem().openBlocking(tempFile.toString(), new OpenOptions());

        Async async = context.async();
        prepareContainer(context)
                // put an object then get/head the object
                .flatMap(new PutObjectStream(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, bigFile)
                        .setHeader(CONTENT_LENGTH, valueOf(size))
                        .setHeader(ETAG, base16().lowerCase().encode(md5))
                        .setHeader(X_SERVER_SIDE_ENCRYPTION, "true"))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, 0, true, 0, md5, sha512, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, 0, true, size, md5, sha512, 1))
                .flatMap(new Func1<HttpClientResponse, Observable<HttpClientResponse>>() {
                    @Override
                    public Observable<HttpClientResponse> call(final HttpClientResponse httpClientResponse) {
                        final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512, MD5);
                        return pump(httpClientResponse, digestWriteStream)
                                .map(new Func1<Void, HttpClientResponse>() {
                                    @Override
                                    public HttpClientResponse call(Void aVoid) {
                                        assertArrayEquals(context, md5, digestWriteStream.getDigest(MD5).get());
                                        assertArrayEquals(context, sha512, digestWriteStream.getDigest(SHA512).get());
                                        return httpClientResponse;
                                    }
                                });
                    }
                })
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testDynamicLargeObject(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO1".getBytes(UTF_8);
        final byte[] data2 = "HELLO2".getBytes(UTF_8);
        final byte[] concated = new byte[data0.length + data1.length + data2.length];
        int position = 0;
        arraycopy(data0, 0, concated, position, data0.length);
        position += data0.length;
        arraycopy(data1, 0, concated, position, data1.length);
        position += data1.length;
        arraycopy(data2, 0, concated, position, data2.length);
        final byte[] contactedMd5 =
                md5().newHasher()
                        .putBytes(md5().hashBytes(data0).asBytes())
                        .putBytes(md5().hashBytes(data1).asBytes())
                        .putBytes(md5().hashBytes(data2).asBytes())
                        .hash()
                        .asBytes();
        final byte[] contactedSha512 =
                sha512().newHasher()
                        .putBytes(sha512().hashBytes(data0).asBytes())
                        .putBytes(sha512().hashBytes(data1).asBytes())
                        .putBytes(sha512().hashBytes(data2).asBytes())
                        .hash()
                        .asBytes();

        Async async = context.async();
        prepareContainer(context)

                // put an object then get/head the object
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/0", authNonAdmin, data0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/1", authNonAdmin, data1))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/2", authNonAdmin, data2))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, new byte[]{})
                        .setHeader(X_OBJECT_MANIFEST, containerName + "/" + objectName + "/segments/"))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, 0, false, concated.length, contactedMd5, contactedSha512, 0))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, concated))
                .map(new ToVoid<Buffer>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testChunkedEncodingEncrypted(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);

        Async async = context.async();
        prepareContainer(context)
                // put an object then get/head the object
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0)
                        .setHeader(X_SERVER_SIDE_ENCRYPTION, valueOf(true))
                        .setChunked(true))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, true, 0, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, true, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, true, data0.length, 2))
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testChunkedEncoding(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);

        Async async = context.async();
        prepareContainer(context)
                // put an object then get/head the object
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0)
                        .setChunked(true))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 3))
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testEncrypted(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);

        Async async = context.async();
        prepareContainer(context)
                // put an object then get/head the object
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0)
                        .setHeader(X_SERVER_SIDE_ENCRYPTION, valueOf(true)))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, true, 0, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, true, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, true, data0.length, 2))
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testExpireTtl(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final long currentTimeInMillis = currentTimeMillis();

        Async async = context.async();
        prepareContainer(context)

                // put an object then get/head the object
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0)
                        .setHeader(X_DELETE_AT, valueOf(currentTimeInMillis)))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testPutGetZeroLength(TestContext context) {
        final byte[] data0 = {};
        Async async = context.async();
        prepareContainer(context)

                // put an object then get/head the object
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 2))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(new Func1<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject call(JsonObject jsonObject) {
                        assertEquals(context, "Version 1 is delete marker", jsonObject.getString("message"));
                        return jsonObject;
                    }
                })
                .map(new ToVoid<JsonObject>())
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(0)))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 2, false, 0, 3))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 2, false, data0.length, 4))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(new Func1<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject call(JsonObject jsonObject) {
                        assertEquals(context, "Object does not exist", jsonObject.getString("message"));
                        return jsonObject;
                    }
                })
                .map(new ToVoid<JsonObject>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testPutPostGetDelete(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO1".getBytes(UTF_8);
        final byte[] data2 = "HELLO2".getBytes(UTF_8);
        final long currentTimeInMillis = currentTimeMillis();

        AtomicInteger counter = new AtomicInteger(0);
        Async async = context.async();
        prepareContainer(context)
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, containerName, authAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(30)))
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                // put an object then get/head the object
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 0))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 2))
                .map(new ToVoid<HttpClientResponse>())

                // delete the object then get/head the object
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(new Func1<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject call(JsonObject jsonObject) {
                        assertEquals(context, "Version 1 is delete marker", jsonObject.getString("message"));
                        return jsonObject;
                    }
                })
                .map(new ToVoid<JsonObject>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())

                // put the object again after deletion then get/head the object
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data1, 2, false, 0, 3))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 2, false, data1.length, 4))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data1))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 2, false, data1.length, 5))
                .map(new ToVoid<HttpClientResponse>())

                // put the object again overwriting the previous
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data2))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data2, 3, false, 0, 6))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data2, 3, false, data2.length, 7))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data2))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data2, 3, false, data2.length, 8))
                .map(new ToVoid<HttpClientResponse>())
                // get old versions by id #3
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(3))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data2, 3, false, data2.length, 9))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data2))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(3))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data2, 3, false, data2.length, 10))
                .map(new ToVoid<HttpClientResponse>())
                // now we'll get each old version by id and assert instead of trying to read the latest version
                // like we did above
                // get old versions by id #2
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(2))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 2, false, data2.length, 11))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data1))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(2))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 2, false, data2.length, 12))
                .map(new ToVoid<HttpClientResponse>())
                // get old versions by id #1
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                // get old versions by id #0
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 13))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 14))
                .map(new ToVoid<HttpClientResponse>())
                // now we'll delete the old versions permanently by referencing them specifically
                // when we don't reference a version specifically a delete marker is created
                // delete version #3
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(3))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(3))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(3))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                // delete version #2
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(2))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(2))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(2))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                // delete version #1
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                // delete version #0
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())

                // now we'll put two versions so we can try delete where versions=all
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 15))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 16))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 17))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data1, 1, false, 0, 18))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 1, false, data1.length, 19))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data1))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 1, false, data1.length, 20))
                .map(new ToVoid<HttpClientResponse>())

                // delete all versions and then make sure they're all deleted
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setAllVersions(true))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                // get version #1
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                // get version #0
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())

                // put two versions after deletion so we can test delete version=0,1
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 21))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 22))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 23))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data1, 1, false, 0, 24))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 1, false, data1.length, 25))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data1))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 1, false, data1.length, 26))
                .map(new ToVoid<HttpClientResponse>())

                // delete all versions and then make sure they're all deleted
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0, 1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                // get version #1
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                // get version #0
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())

                // put an object and then test updating updateable properties
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new AssertObjectHeaders(context, data0, 0, false, 0, 27))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new IndexRefresh(VERTX_CONTEXT))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 28))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 29))
                .map(new ToVoid<HttpClientResponse>())
                // now we update updateable properties
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PostObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setHeader(CONTENT_ENCODING, "gzip")
                        .setHeader(CONTENT_TYPE, "text/xml")
                        .setHeader(CONTENT_DISPOSITION, "inline; filename=\"foo.html\"")
                        .setHeader(X_DELETE_AT, valueOf(currentTimeInMillis + 1200000))
                        .setHeader(X_ADD_OBJECT_META_PREFIX + "my_test_metatadata", "abc123"))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_ACCEPTED))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 1, false, data0.length, 30))
                .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                    @Override
                    public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                        MultiMap headers = httpClientResponse.headers();
                        String contentEncoding = headers.get(CONTENT_ENCODING);
                        String contentType = headers.get(CONTENT_TYPE);
                        String contentDisposition = headers.get(CONTENT_DISPOSITION);
                        String deleteAt = headers.get(X_DELETE_AT);
                        String myTestMetadata = headers.get(X_ADD_OBJECT_META_PREFIX + "my_test_metatadata");

                        assertEquals(context, "gzip", contentEncoding);
                        assertEquals(context, "text/xml", contentType);
                        assertEquals(context, "inline; filename=\"foo.html\"", contentDisposition);
                        assertEquals(context, valueOf(currentTimeInMillis + 1200000), deleteAt);
                        assertEquals(context, "abc123", myTestMetadata);

                        return httpClientResponse;
                    }
                })
                .map(new ToVoid<HttpClientResponse>())
                // now we update updateable properties again to make sure metadata and properties
                // not specified get deleted instead of propagating from the previous version
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new PostObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setHeader(CONTENT_ENCODING, "zipper")
                        .setHeader(X_ADD_OBJECT_META_PREFIX + "my_test_metatadata2", "abc321"))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_ACCEPTED))
                .map(new ToVoid<HttpClientResponse>())
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 2, false, data0.length, 21))
                .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                    @Override
                    public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                        MultiMap headers = httpClientResponse.headers();
                        String contentEncoding = headers.get(CONTENT_ENCODING);
                        String contentType = headers.get(CONTENT_TYPE);
                        String contentDisposition = headers.get(CONTENT_DISPOSITION);
                        String deleteAt = headers.get(X_DELETE_AT);
                        String myTestMetadata = headers.get(X_ADD_OBJECT_META_PREFIX + "my_test_metatadata");
                        String myTestMetadata2 = headers.get(X_ADD_OBJECT_META_PREFIX + "my_test_metatadata2");

                        assertEquals(context, "zipper", contentEncoding);
                        assertEquals(context, "application/octet-stream", contentType);
                        assertNull(context, contentDisposition);
                        assertNull(context, deleteAt);
                        assertNull(context, myTestMetadata);
                        assertEquals(context, "abc321", myTestMetadata2);

                        return httpClientResponse;
                    }
                })
                .map(new ToVoid<HttpClientResponse>())
                // recheck that the previous version still has the properties
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new HeadObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 1, false, data0.length, 32))
                .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                    @Override
                    public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                        MultiMap headers = httpClientResponse.headers();
                        String contentEncoding = headers.get(CONTENT_ENCODING);
                        String contentType = headers.get(CONTENT_TYPE);
                        String contentDisposition = headers.get(CONTENT_DISPOSITION);
                        String deleteAt = headers.get(X_DELETE_AT);
                        String myTestMetadata = headers.get(X_ADD_OBJECT_META_PREFIX + "my_test_metatadata");

                        assertEquals(context, "gzip", contentEncoding);
                        assertEquals(context, "text/xml", contentType);
                        assertEquals(context, "inline; filename=\"foo.html\"", contentDisposition);
                        assertEquals(context, valueOf(currentTimeInMillis + 1200000), deleteAt);
                        assertEquals(context, "abc123", myTestMetadata);

                        return httpClientResponse;
                    }
                })
                .map(new ToVoid<HttpClientResponse>())
                // check that the version #2 still has the data from version #0
                .doOnNext(aData -> out.println("Count = " + counter.getAndIncrement()))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 2, false, data0.length, 33))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<Buffer>())
                .subscribe(new TestSubscriber(context, async));
    }


}
