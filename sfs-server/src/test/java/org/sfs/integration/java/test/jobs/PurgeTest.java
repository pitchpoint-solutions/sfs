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

package org.sfs.integration.java.test.jobs;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PostContainer;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.integration.java.func.VerifyRepairAllContainersExecute;
import org.sfs.jobs.Jobs;
import org.sfs.jobs.VerifyRepairAllContainerObjects;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;
import rx.functions.Func1;

import java.util.Calendar;

import static com.google.common.collect.ArrayListMultimap.create;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.util.Calendar.getInstance;
import static org.sfs.filesystem.volume.VolumeV1.TINY_DATA_THRESHOLD;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.KnownMetadataKeys.X_MAX_OBJECT_REVISIONS;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META_PREFIX;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertFalse;
import static org.sfs.util.VertxAssert.assertTrue;
import static org.sfs.vo.ObjectPath.fromPaths;
import static rx.Observable.just;

public class PurgeTest extends BaseTestVerticle {

    private final String accountName = "testaccount";
    private final String containerName = "testcontainer";
    private final String objectName = "testobject";

    private Producer authAdmin = httpBasic("admin", "admin");
    private Producer authNonAdmin = httpBasic("user", "user");

    protected Observable<Void> prepareContainer(TestContext context) {
        return just((Void) null)
                .flatMap(new PostAccount(httpClient(), accountName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutContainer(httpClient(), accountName, containerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<HttpClientResponse>())
                .count()
                .map(new ToVoid<Integer>());
    }

    @Test
    public void testForceRemoveVolume(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);

            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 2, versions.size());
                        return jsonObject;
                    })
                    .flatMap(jsonObject -> {
                        Calendar past = getInstance();
                        past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        IndexRequestBuilder request = elasticsearch.get()
                                .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        jsonObject.put("update_ts", toDateTimeString(past));
                        request.setSource(jsonObject.encode());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                .map(indexResponseOptional -> jsonObject);
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin)
                            .setHeader(Jobs.Parameters.FORCE_REMOVE_VOLUMES, Iterables.getFirst(vertxContext().verticle().nodes().volumeManager().volumes(), null)))
                    .map(new ToVoid<>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        assertFalse(context, getResponse.isExists());
                        return getResponse;
                    })
                    .map(new ToVoid<GetResponse>());
        });
    }

    @Test
    public void testPurgeExpiredTwoVersionsNoVersionsRemaining(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);
            final long currentTimeInMillis = currentTimeMillis();

            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 2, versions.size());
                        return jsonObject;
                    })
                    .flatMap(jsonObject -> {
                        Calendar past = getInstance();
                        past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        IndexRequestBuilder request = elasticsearch.get()
                                .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        for (Object o : versions) {
                            JsonObject jsonVersion = (JsonObject) o;
                            jsonVersion.put("delete_at", currentTimeInMillis);
                        }
                        jsonObject.put("update_ts", toDateTimeString(past));
                        request.setSource(jsonObject.encode());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                .map(indexResponseOptional -> jsonObject);
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        assertFalse(context, getResponse.isExists());
                        return getResponse;
                    })
                    .map(new ToVoid<GetResponse>());
        });
    }

    @Test
    public void testPurgeExpiredTwoVersionsOneVersionRemaining(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);
            final long currentTimeInMillis = currentTimeMillis();

            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 2, versions.size());
                        return jsonObject;
                    })
                    .flatMap(jsonObject -> {
                        Calendar past = getInstance();
                        past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        IndexRequestBuilder request = elasticsearch.get()
                                .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        for (Object o : versions) {
                            JsonObject jsonVersion = (JsonObject) o;
                            jsonVersion.put("delete_at", currentTimeInMillis);
                            break;
                        }
                        jsonObject.put("update_ts", toDateTimeString(past));
                        request.setSource(jsonObject.encode());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                .map(indexResponseOptional -> jsonObject);
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        assertTrue(context, getResponse.isExists());
                        return getResponse;
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 1, versions.size());
                        return jsonObject;
                    })
                    .map(new ToVoid<JsonObject>());
        });
    }

    @Test
    public void testPurgeDeletedTwoVersionsNoVersionsRemaining(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);

            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 2, versions.size());
                        return jsonObject;
                    })
                    .flatMap(jsonObject -> {
                        Calendar past = getInstance();
                        past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        IndexRequestBuilder request = elasticsearch.get()
                                .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        for (Object o : versions) {
                            JsonObject jsonVersion = (JsonObject) o;
                            jsonVersion.put("deleted", true);
                        }
                        jsonObject.put("update_ts", toDateTimeString(past));
                        request.setSource(jsonObject.encode());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                .map(indexResponseOptional -> jsonObject);
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(getResponse -> {
                        assertFalse(context, getResponse.isExists());
                        return getResponse;
                    })
                    .map(new ToVoid<GetResponse>());
        });
    }

    @Test
    public void testPurgeDeletedTwoVersionsOneVersionRemaining(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);

            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 2, versions.size());
                        return jsonObject;
                    })
                    .flatMap(jsonObject -> {
                        Calendar past = getInstance();
                        past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        IndexRequestBuilder request = elasticsearch.get()
                                .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        for (Object o : versions) {
                            JsonObject jsonVersion = (JsonObject) o;
                            jsonVersion.put("deleted", true);
                            break;
                        }
                        jsonObject.put("update_ts", toDateTimeString(past));
                        request.setSource(jsonObject.encode());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                .map(indexResponseOptional -> jsonObject);
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    })
                    .map(getResponse -> {
                        assertTrue(context, getResponse.isExists());
                        return getResponse;
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 1, versions.size());
                        return jsonObject;
                    })
                    .map(new ToVoid<JsonObject>());
        });
    }

    @Test
    public void testPurgeNotVerifiedTwoVersionsNoVersionsRemaining(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);

            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    })
                    .map(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        assertEquals(context, 2, versions.size());
                        return jsonObject;
                    })
                    .flatMap(jsonObject -> {
                        Calendar past = getInstance();
                        past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        IndexRequestBuilder request = elasticsearch.get()
                                .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                        // two things need to happen here
                        // 1. set verified to false so the sweep finds the record
                        // 2. set a acknowledged to false so that when the "Cleaner" logic looks at the data it decides that this record needs removing

                        for (Object o : versions) {
                            JsonObject jsonVersion = (JsonObject) o;
                            jsonVersion.put("verified", false);
                            JsonArray segments = jsonVersion.getJsonArray("segments", new JsonArray());
                            for (Object p : segments) {
                                JsonObject jsonSegment = (JsonObject) p;
                                JsonArray blobs = jsonSegment.getJsonArray("blobs", new JsonArray());
                                for (Object q : blobs) {
                                    JsonObject jsonBlob = (JsonObject) q;
                                    jsonBlob.put("acknowledged", false);
                                    jsonBlob.put("verify_fail_count", VerifyRepairAllContainerObjects.VERIFY_RETRY_COUNT);
                                }
                            }
                        }
                        jsonObject.put("update_ts", toDateTimeString(past));
                        request.setSource(jsonObject.encode());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                .map(indexResponseOptional -> jsonObject);
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(Optional::get);
                        }
                    })
                    .map(new Func1<GetResponse, GetResponse>() {
                        @Override
                        public GetResponse call(GetResponse getResponse) {
                            assertFalse(context, getResponse.isExists());
                            return getResponse;
                        }
                    })
                    .map(new ToVoid<GetResponse>());
        });
    }

    @Test
    public void testPurgeNoVerifiedTwoVersionsTwoVersionRemaining(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);

            return prepareContainer(context)
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(new Func1<Optional<GetResponse>, GetResponse>() {
                                        @Override
                                        public GetResponse call(Optional<GetResponse> getResponseOptional) {
                                            return getResponseOptional.get();
                                        }
                                    });
                        }
                    })
                    .map(new Func1<GetResponse, JsonObject>() {
                        @Override
                        public JsonObject call(GetResponse getResponse) {
                            JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            assertEquals(context, 2, versions.size());
                            return jsonObject;
                        }
                    })
                    .flatMap(new Func1<JsonObject, Observable<JsonObject>>() {
                        @Override
                        public Observable<JsonObject> call(final JsonObject jsonObject) {
                            Calendar past = getInstance();
                            past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            IndexRequestBuilder request = elasticsearch.get()
                                    .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            // two things need to happen here
                            // 1. set verified to false so the sweep finds the record
                            // 2. set a acknowledged to false so that when the "Cleaner" logic looks at the data it decides that this record needs removing

                            for (Object o : versions) {
                                JsonObject jsonVersion = (JsonObject) o;
                                jsonVersion.put("verified", false);
                                JsonArray segments = jsonVersion.getJsonArray("segments", new JsonArray());
                                for (Object p : segments) {
                                    JsonObject jsonSegment = (JsonObject) p;
                                    JsonArray blobs = jsonSegment.getJsonArray("blobs", new JsonArray());
                                    for (Object q : blobs) {
                                        JsonObject jsonBlob = (JsonObject) q;
                                        jsonBlob.put("acknowledged", false);
                                        jsonBlob.put("verify_fail_count", 0);
                                    }
                                }
                                break;
                            }
                            jsonObject.put("update_ts", toDateTimeString(past));
                            request.setSource(jsonObject.encode());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                    .map(new Func1<Optional<IndexResponse>, JsonObject>() {
                                        @Override
                                        public JsonObject call(Optional<IndexResponse> indexResponseOptional) {
                                            return jsonObject;
                                        }
                                    });
                        }
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
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
                    .map(new Func1<GetResponse, JsonObject>() {
                        @Override
                        public JsonObject call(GetResponse getResponse) {
                            JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            assertEquals(context, 2, versions.size());
                            return jsonObject;
                        }
                    })
                    .map(new ToVoid<JsonObject>());
        });
    }

    @Test
    public void testPurgeNoVerifiedTwoVersionsOneVersionRemaining(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);

            return prepareContainer(context)
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(new Func1<Optional<GetResponse>, GetResponse>() {
                                        @Override
                                        public GetResponse call(Optional<GetResponse> getResponseOptional) {
                                            return getResponseOptional.get();
                                        }
                                    });
                        }
                    })
                    .map(new Func1<GetResponse, JsonObject>() {
                        @Override
                        public JsonObject call(GetResponse getResponse) {
                            JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            assertEquals(context, 2, versions.size());
                            return jsonObject;
                        }
                    })
                    .flatMap(new Func1<JsonObject, Observable<JsonObject>>() {
                        @Override
                        public Observable<JsonObject> call(final JsonObject jsonObject) {
                            Calendar past = getInstance();
                            past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            IndexRequestBuilder request = elasticsearch.get()
                                    .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            // two things need to happen here
                            // 1. set verified to false so the sweep finds the record
                            // 2. set a acknowledged to false so that when the "Cleaner" logic looks at the data it decides that this record needs removing

                            for (Object o : versions) {
                                JsonObject jsonVersion = (JsonObject) o;
                                jsonVersion.put("verified", false);
                                JsonArray segments = jsonVersion.getJsonArray("segments", new JsonArray());
                                for (Object p : segments) {
                                    JsonObject jsonSegment = (JsonObject) p;
                                    JsonArray blobs = jsonSegment.getJsonArray("blobs", new JsonArray());
                                    for (Object q : blobs) {
                                        JsonObject jsonBlob = (JsonObject) q;
                                        jsonBlob.put("acknowledged", false);
                                        jsonBlob.put("verify_fail_count", VerifyRepairAllContainerObjects.VERIFY_RETRY_COUNT);
                                    }
                                }
                                break;
                            }
                            jsonObject.put("update_ts", toDateTimeString(past));
                            request.setSource(jsonObject.encode());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                    .map(new Func1<Optional<IndexResponse>, JsonObject>() {
                                        @Override
                                        public JsonObject call(Optional<IndexResponse> indexResponseOptional) {
                                            return jsonObject;
                                        }
                                    });
                        }
                    })
                    .map(new ToVoid<JsonObject>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
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
                    .map(new Func1<GetResponse, JsonObject>() {
                        @Override
                        public JsonObject call(GetResponse getResponse) {
                            JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            assertEquals(context, 1, versions.size());
                            return jsonObject;
                        }
                    })
                    .map(new ToVoid<JsonObject>());
        });
    }

    @Test
    public void testPurgeAboveVersionLimit(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);

            ListMultimap<String, String> headers = create();
            headers.put(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(2));

            return prepareContainer(context)
                    .flatMap(new PostContainer(httpClient(), accountName, containerName, authAdmin, headers))
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new Func1<Void, Observable<GetResponse>>() {
                        @Override
                        public Observable<GetResponse> call(Void aVoid) {
                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            GetRequestBuilder request = elasticsearch.get()
                                    .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                    .map(new Func1<Optional<GetResponse>, GetResponse>() {
                                        @Override
                                        public GetResponse call(Optional<GetResponse> getResponseOptional) {
                                            return getResponseOptional.get();
                                        }
                                    });
                        }
                    })
                    .map(new Func1<GetResponse, JsonObject>() {
                        @Override
                        public JsonObject call(GetResponse getResponse) {
                            JsonObject jsonObject = new JsonObject(getResponse.getSourceAsString());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            assertEquals(context, 2, versions.size());
                            return jsonObject;
                        }
                    })
                    .flatMap(new Func1<JsonObject, Observable<JsonObject>>() {
                        @Override
                        public Observable<JsonObject> call(final JsonObject jsonObject) {
                            Calendar past = getInstance();
                            past.setTimeInMillis(System.currentTimeMillis() - (VerifyRepairAllContainerObjects.CONSISTENCY_THRESHOLD * 2));

                            Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                            IndexRequestBuilder request = elasticsearch.get()
                                    .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                            JsonArray versions = jsonObject.getJsonArray("versions", new JsonArray());
                            // two things need to happen here
                            // 1. set verified to false so the sweep finds the record
                            // 2. set a acknowledged to false so that when the "Cleaner" logic looks at the data it decides that this record needs removing

                            for (Object o : versions) {
                                JsonObject jsonVersion = (JsonObject) o;
                                int versionId = jsonVersion.getInteger("id");
                                switch (versionId) {
                                    case 2:
                                        break;
                                    case 3:
                                        break;
                                    default:
                                        context.fail("Versions must be 2 or 3. 0 and 1 should have been deleted because max revisions is 2");
                                }
                            }
                            jsonObject.put("update_ts", toDateTimeString(past));
                            request.setSource(jsonObject.encode());
                            return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                    .map(new Func1<Optional<IndexResponse>, JsonObject>() {
                                        @Override
                                        public JsonObject call(Optional<IndexResponse> indexResponseOptional) {
                                            return jsonObject;
                                        }
                                    });
                        }
                    })
                    .map(new ToVoid<JsonObject>());
        });
    }

}
