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

package org.sfs.integration.java.test.container;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.elasticsearch.object.LoadAccountAndContainerAndObject;
import org.sfs.elasticsearch.object.UpdateObject;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.AssertObjectData;
import org.sfs.integration.java.func.AssertObjectHeaders;
import org.sfs.integration.java.func.ContainerExport;
import org.sfs.integration.java.func.ContainerImport;
import org.sfs.integration.java.func.DeleteContainer;
import org.sfs.integration.java.func.DeleteObject;
import org.sfs.integration.java.func.GetContainer;
import org.sfs.integration.java.func.GetObject;
import org.sfs.integration.java.func.HttpClientResponseAndBuffer;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PostContainer;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.rx.BufferToJsonArray;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;
import org.sfs.validate.ValidateOptimisticObjectLock;
import org.sfs.vo.PersistentObject;
import rx.Observable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.MediaType.OCTET_STREAM;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static java.lang.String.valueOf;
import static java.lang.System.arraycopy;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.file.Files.createTempDirectory;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.KnownMetadataKeys.X_MAX_OBJECT_REVISIONS;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META_PREFIX;
import static org.sfs.util.SfsHttpHeaders.X_OBJECT_MANIFEST;
import static org.sfs.util.SfsHttpHeaders.X_SFS_COMPRESS;
import static org.sfs.util.SfsHttpHeaders.X_SFS_SECRET;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.vo.ObjectPath.fromPaths;
import static org.sfs.vo.Segment.EMPTY_MD5;
import static rx.Observable.just;

public class ContainerExportImportTest extends BaseTestVerticle {


    private final String accountName = "testaccount";
    private final String containerName = "testcontainer";
    private final String objectName = "testobject";

    private Producer authAdmin = httpBasic("admin", "admin");
    private Producer authNonAdmin = httpBasic("user", "user");


    protected Observable<Void> prepareContainer(TestContext context) {

        return just((Void) null)
                .flatMap(new PostAccount(HTTP_CLIENT, accountName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(3)))
                .map(new ToVoid<>());
    }

    @Test
    public void testExportImportOneVersionNoCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportOneVersion(context, false, false);
    }

    @Test
    public void testExportImportOneVersionCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportOneVersion(context, true, false);
    }

    @Test
    public void testExportImportOneVersionCompressEncrypt(TestContext context) throws IOException {
        testExportImportOneVersion(context, true, true);
    }

    public void testExportImportOneVersion(TestContext context, boolean compress, boolean encrypt) throws IOException {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO1".getBytes(UTF_8);
        final byte[] data2 = "HELLO2".getBytes(UTF_8);

        byte[] secret = new byte[32];
        if (encrypt) {
            getCurrentInstance().nextBytesBlocking(secret);
        }

        final String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        final String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());
        final String md52 = base16().lowerCase().encode(md5().hashBytes(data2).asBytes());

        String importContainerName = "import-container";

        Path exportDirectory = createTempDirectory(tmpDir, "");

        Async async = context.async();
        prepareContainer(context)

                // put three objects then list and assert
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/4", authNonAdmin, new byte[]{}))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/3", authNonAdmin, data2)
                        .setHeader(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString()))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/2", authNonAdmin, data1))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/1", authNonAdmin, data0))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> containerExport(exportDirectory, compress, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(aVoid -> containerImport(exportDirectory, importContainerName, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetContainer(HTTP_CLIENT, accountName, importContainerName, authAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonArray())
                .map(jsonArray -> {
                    assertEquals(context, 4, jsonArray.size());
                    for (Object o : jsonArray) {
                        JsonObject jsonObject = (JsonObject) o;
                        String name = jsonObject.getString("name");
                        String hash = jsonObject.getString("hash");
                        String mediaType = jsonObject.getString("content_type");
                        long bytes = jsonObject.getLong("bytes");

                        switch (name) {
                            case objectName + "/4":
                                assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                assertEquals(context, 0, bytes);
                                break;
                            case objectName + "/3":
                                assertEquals(context, md52, hash);
                                assertEquals(context, PLAIN_TEXT_UTF_8.toString(), mediaType);
                                assertEquals(context, data2.length, bytes);
                                break;
                            case objectName + "/2":
                                assertEquals(context, md51, hash);
                                assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                assertEquals(context, data1.length, bytes);
                                break;
                            case objectName + "/1":
                                assertEquals(context, md50, hash);
                                assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                assertEquals(context, data0.length, bytes);
                                break;
                            default:
                                context.fail();
                        }
                    }
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }


    @Test
    public void testExportImportTwoVersionsNoCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportTwoVersions(context, false, false);
    }

    @Test
    public void testExportImportTwoVersionsCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportTwoVersions(context, true, false);
    }

    @Test
    public void testExportImportTwoVersionsCompressEncrypt(TestContext context) throws IOException {
        testExportImportTwoVersions(context, true, true);
    }

    public void testExportImportTwoVersions(TestContext context, boolean compress, boolean encrypt) throws IOException {
        byte[] data0 = "HELLO0".getBytes(UTF_8);
        byte[] data1 = "HELLO1".getBytes(UTF_8);

        byte[] secret = new byte[32];
        if (encrypt) {
            getCurrentInstance().nextBytesBlocking(secret);
        }

        String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());

        String importContainerName = "import-container";

        Path exportDirectory = createTempDirectory(tmpDir, "");

        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data1))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> containerExport(exportDirectory, compress, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(aVoid -> containerImport(exportDirectory, importContainerName, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetContainer(HTTP_CLIENT, accountName, importContainerName, authAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonArray())
                .map(jsonArray -> {
                    assertEquals(context, 1, jsonArray.size());
                    for (Object o : jsonArray) {
                        JsonObject jsonObject = (JsonObject) o;
                        String name = jsonObject.getString("name");
                        String hash = jsonObject.getString("hash");
                        String mediaType = jsonObject.getString("content_type");
                        long bytes = jsonObject.getLong("bytes");

                        switch (name) {
                            case objectName:
                                assertEquals(context, md51, hash);
                                assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                assertEquals(context, data1.length, bytes);
                                break;
                            default:
                                context.fail();
                        }
                    }
                    return (Void) null;
                })
                .flatMap(new GetObject(HTTP_CLIENT, accountName, importContainerName, objectName, authAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, importContainerName, objectName, authAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data1, 1, false, data1.length, 2))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data1))
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testExportImportDeleteMarkerNoCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportDeleteMarker(context, false, false);
    }

    @Test
    public void testExportImportDeleteMarkerCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportDeleteMarker(context, true, false);
    }

    @Test
    public void testExportImportDeleteMarkerCompressEncrypt(TestContext context) throws IOException {
        testExportImportDeleteMarker(context, true, true);
    }

    public void testExportImportDeleteMarker(TestContext context, boolean compress, boolean encrypt) throws IOException {
        byte[] data0 = "HELLO0".getBytes(UTF_8);

        byte[] secret = new byte[32];
        if (encrypt) {
            getCurrentInstance().nextBytesBlocking(secret);
        }

        String importContainerName = "import-container";

        Path exportDirectory = createTempDirectory(tmpDir, "");

        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new ToVoid<>())
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> containerExport(exportDirectory, compress, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(3)))
                .map(new ToVoid<>())
                .flatMap(aVoid -> containerImport(exportDirectory, importContainerName, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetContainer(HTTP_CLIENT, accountName, importContainerName, authAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonArray())
                .map(jsonArray -> {
                    assertEquals(context, 0, jsonArray.size());
                    return (Void) null;
                })
                .flatMap(new GetObject(HTTP_CLIENT, accountName, importContainerName, objectName, authAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, importContainerName, objectName, authAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testExportImportDeletedNoCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportDeleted(context, false, false);
    }

    @Test
    public void testExportImportDeletedCompressNoEncrypt(TestContext context) throws IOException {
        testExportImportDeleted(context, true, false);
    }

    @Test
    public void testExportImportDeletedCompressEncrypt(TestContext context) throws IOException {
        testExportImportDeleted(context, true, true);
    }

    public void testExportImportDeleted(TestContext context, boolean compress, boolean encrypt) throws IOException {
        byte[] data0 = "HELLO0".getBytes(UTF_8);
        byte[] data1 = "HELLO1".getBytes(UTF_8);

        String importContainerName = "import-container";

        Path exportDirectory = createTempDirectory(tmpDir, "");

        byte[] secret = new byte[32];
        if (encrypt) {
            getCurrentInstance().nextBytesBlocking(secret);
        }

        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data1))
                .map(new ToVoid<>())
                .map(aVoid -> fromPaths(accountName, containerName, objectName))
                .flatMap(new LoadAccountAndContainerAndObject(vertxContext()))
                .map(persistentObject ->
                        persistentObject.getNewestVersion()
                                .get().setDeleted(true))
                .map(version -> (PersistentObject) version.getParent())
                .flatMap(new UpdateObject(vertxContext()))
                .map(new ValidateOptimisticObjectLock())
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> containerExport(exportDirectory, compress, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(3)))
                .map(new ToVoid<>())
                .flatMap(aVoid -> containerImport(exportDirectory, importContainerName, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetContainer(HTTP_CLIENT, accountName, importContainerName, authAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonArray())
                .map(jsonArray -> {
                    assertEquals(context, 0, jsonArray.size());
                    return (Void) null;
                })
                .flatMap(new GetObject(HTTP_CLIENT, accountName, importContainerName, objectName, authAdmin)
                        .setVersion(0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, data0, 0, false, data0.length, 1))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, data0))
                .map(new ToVoid<>())
                .flatMap(new GetObject(HTTP_CLIENT, accountName, importContainerName, objectName, authAdmin)
                        .setVersion(1))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testDynamicLargeObjectNoCompressionNoEncrypt(TestContext context) throws IOException {
        testDynamicLargeObject(context, false, false);
    }

    @Test
    public void testDynamicLargeObjectCompressionNoEncrypt(TestContext context) throws IOException {
        testDynamicLargeObject(context, true, false);
    }

    @Test
    public void testDynamicLargeObjectCompressionEncrypt(TestContext context) throws IOException {
        testDynamicLargeObject(context, true, true);
    }

    public void testDynamicLargeObject(TestContext context, boolean compress, boolean encrypt) throws IOException {
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

        String importContainerName = "import-container";

        Path exportDirectory = createTempDirectory(tmpDir, "");

        byte[] secret = new byte[32];
        if (encrypt) {
            getCurrentInstance().nextBytesBlocking(secret);
        }

        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/0", authNonAdmin, data0))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/1", authNonAdmin, data1))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/2", authNonAdmin, data2))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, new byte[]{})
                        .setHeader(X_OBJECT_MANIFEST, containerName + "/" + objectName + "/segments/"))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> containerExport(exportDirectory, compress, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/0", authNonAdmin)
                        .setAllVersions(true))
                .map(new ToVoid<>())
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/1", authNonAdmin)
                        .setAllVersions(true))
                .map(new ToVoid<>())
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName + "/segments/2", authNonAdmin)
                        .setAllVersions(true))
                .map(new ToVoid<>())
                .flatMap(new DeleteObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin)
                        .setAllVersions(true))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new DeleteContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin))
                .map(new ToVoid<>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, importContainerName, authNonAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(3)))
                .map(new ToVoid<>())
                .flatMap(aVoid -> containerImport(exportDirectory, importContainerName, encrypt, secret))
                .doOnNext(httpClientResponseAndBuffer -> {
                    assertEquals(context, HTTP_OK, httpClientResponseAndBuffer.getHttpClientResponse().statusCode());
                    JsonObject jsonObject = new JsonObject(httpClientResponseAndBuffer.getBuffer().toString(StandardCharsets.UTF_8));
                    assertEquals(context, (Integer) HTTP_OK, jsonObject.getInteger("code"));
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetObject(HTTP_CLIENT, accountName, importContainerName, objectName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .map(new AssertObjectHeaders(context, 0, false, concated.length, contactedMd5, contactedSha512, 0))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new AssertObjectData(context, concated))
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    protected Observable<HttpClientResponseAndBuffer> containerImport(Path importDirectory, String importContainerName, boolean encrypt, byte[] secret) {
        ContainerImport func = new ContainerImport(HTTP_CLIENT, accountName, importContainerName, importDirectory, authAdmin);
        if (encrypt) {
            func.setHeader(X_SFS_SECRET, base64().encode(secret));
        }
        return func.call(null);
    }

    protected Observable<HttpClientResponseAndBuffer> containerExport(Path exportDirectory, boolean compress, boolean encrypt, byte[] secret) {
        ContainerExport func = new ContainerExport(HTTP_CLIENT, accountName, containerName, exportDirectory, authAdmin);
        if (compress) {
            func.setHeader(X_SFS_COMPRESS, "true");
        }
        if (encrypt) {
            func.setHeader(X_SFS_SECRET, base64().encode(secret));
        }
        return func.call(null);
    }

}
