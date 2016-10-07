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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.integration.java.func.RunJobs;
import org.sfs.nodes.Nodes;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpClientResponseHeaderLogger;
import org.sfs.vo.TransientBlobReference;
import org.sfs.vo.TransientSegment;
import org.sfs.vo.TransientVersion;
import rx.Observable;

import java.util.Calendar;
import java.util.List;
import java.util.NavigableSet;

import static com.google.common.collect.Iterables.size;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.getInstance;
import static org.sfs.elasticsearch.object.MaintainObjectsForNode.CONSISTENCY_THRESHOLD;
import static org.sfs.filesystem.volume.VolumeV1.TINY_DATA_THRESHOLD;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.vo.ObjectPath.fromPaths;
import static org.sfs.vo.PersistentObject.fromGetResponse;
import static rx.Observable.just;

public class BalanceUpTest extends BaseTestVerticle {

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
                .count()
                .map(new ToVoid<>());
    }

    @Test
    public void testIncreaseDecreaseNumberOfReplicas(TestContext context) {
        byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
        getCurrentInstance().nextBytes(data0);
        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().jobs().stop(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // confirm this are in the state they should be before
                    // we fiddle with the replica count
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(segment.verifiedAckdReplicaBlobs()));
                        assertEquals(context, 0, size(segment.verifiedReplicaBlobs()));
                    }
                    return persistentObject;
                })
                .map(aVoid -> {
                    Nodes nodes = VERTX_CONTEXT.verticle().nodes();
                    nodes.setNumberOfReplicas(4)
                            .setAllowSameNode(true);
                    return (Void) null;
                })
                .flatMap(aVoid -> {
                    // create 4 replica volumes
                    VolumeManager volumeManager = VERTX_CONTEXT.verticle().nodes().volumeManager();
                    return volumeManager.newVolume(VERTX_CONTEXT)
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()));


                })
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceNodeStatsUpdate(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> new JsonObject(getResponse.getSourceAsString()))
                .flatMap(jsonObject -> {
                    Calendar past = getInstance();
                    past.add(MILLISECOND, -(CONSISTENCY_THRESHOLD * 2));

                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    IndexRequestBuilder request = elasticsearch.get()
                            .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    jsonObject.put("update_ts", toDateTimeString(past));
                    request.setSource(jsonObject.encode());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultIndexTimeout())
                            .map(indexResponseOptional -> jsonObject);
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceClusterInfoRefresh(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // First time through the blobs won't be acked so assert that they're not.
                    // The blobs aren't acked the first time since they are modified as part
                    // of a bulk index update which can fail for any number of reasons. If we
                    // ack the blobs as part of this index update it becomes possible for the
                    // associated volume to be acked but not be referenced in the index which
                    // cause the block ranges allocated in the volume to forever remain allocated
                    // or a least until we write some code that scans the volumes and checks
                    // if the object has a reference to it's block range
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // second time through the blobs will be acked so assert that they are
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 4, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .map(aVoid -> {
                    Nodes nodes = VERTX_CONTEXT.verticle().nodes();
                    nodes.setNumberOfReplicas(0)
                            .setAllowSameNode(true);
                    return (Void) null;
                })
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceClusterInfoRefresh(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // total number of blobs will be one primary + 4 replicas that were marked deleted
                    // but not removed from the index. Next time the job runs they will be removed from the index.
                    // This is again done so that we don't end up with orphaned block ranges in the volumes
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 5, totalBlobs.size());
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // total number of blobs will be one primary + 4 replicas that were marked deleted
                    // but not removed from the index. Next time the job runs they will be removed from the index.
                    // This is again done so that we don't end up with orphaned block ranges in the volumes
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, totalBlobs.size());
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testIncreaseDecreaseNumberOfPrimaries(TestContext context) {
        byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
        getCurrentInstance().nextBytes(data0);
        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().jobs().stop(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // confirm this are in the state they should be before
                    // we fiddle with the replica count
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(segment.verifiedAckdReplicaBlobs()));
                        assertEquals(context, 0, size(segment.verifiedReplicaBlobs()));
                    }
                    return persistentObject;
                })
                .map(aVoid -> {
                    Nodes nodes = VERTX_CONTEXT.verticle().nodes();
                    nodes.setNumberOfPrimaries(5)
                            .setAllowSameNode(true);
                    return (Void) null;
                })
                .flatMap(aVoid -> {
                    // create 4 replica volumes
                    VolumeManager volumeManager = VERTX_CONTEXT.verticle().nodes().volumeManager();
                    return volumeManager.newVolume(VERTX_CONTEXT)
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setPrimary(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setPrimary(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setPrimary(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setPrimary(VERTX_CONTEXT.vertx()));


                })
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceNodeStatsUpdate(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> new JsonObject(getResponse.getSourceAsString()))
                .flatMap(jsonObject -> {
                    Calendar past = getInstance();
                    past.add(MILLISECOND, -(CONSISTENCY_THRESHOLD * 2));

                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    IndexRequestBuilder request = elasticsearch.get()
                            .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    jsonObject.put("update_ts", toDateTimeString(past));
                    request.setSource(jsonObject.encode());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultIndexTimeout())
                            .map(indexResponseOptional -> jsonObject);
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceClusterInfoRefresh(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // First time through the blobs won't be acked so assert that they're not.
                    // The blobs aren't acked the first time since they are modified as part
                    // of a bulk index update which can fail for any number of reasons. If we
                    // ack the blobs as part of this index update it becomes possible for the
                    // associated volume to be acked but not be referenced in the index which
                    // cause the block ranges allocated in the volume to forever remain allocated
                    // or a least until we write some code that scans the volumes and checks
                    // if the object has a reference to it's block range
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // second time through the blobs will be acked so assert that they are
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 5, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .map(aVoid -> {
                    Nodes nodes = VERTX_CONTEXT.verticle().nodes();
                    nodes.setNumberOfPrimaries(2)
                            .setAllowSameNode(true);
                    return (Void) null;
                })
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceClusterInfoRefresh(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // total number of blobs will be one primary + 4 replicas that were marked deleted
                    // but not removed from the index. Next time the job runs they will be removed from the index.
                    // This is again done so that we don't end up with orphaned block ranges in the volumes
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 5, totalBlobs.size());
                        assertEquals(context, 2, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // second time through the blobs will be acked so assert that they are
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 2, totalBlobs.size());
                        assertEquals(context, 2, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testFixCorruptReplica(TestContext context) {
        byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
        getCurrentInstance().nextBytes(data0);
        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().jobs().stop(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // confirm this are in the state they should be before
                    // we fiddle with the replica count
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(segment.verifiedAckdReplicaBlobs()));
                        assertEquals(context, 0, size(segment.verifiedReplicaBlobs()));
                    }
                    return persistentObject;
                })
                .map(aVoid -> {
                    Nodes nodes = VERTX_CONTEXT.verticle().nodes();
                    nodes.setNumberOfReplicas(1)
                            .setAllowSameNode(true);
                    return (Void) null;
                })
                .flatMap(aVoid -> {
                    // create 4 replica volumes
                    VolumeManager volumeManager = VERTX_CONTEXT.verticle().nodes().volumeManager();
                    return volumeManager.newVolume(VERTX_CONTEXT)
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()));


                })
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceNodeStatsUpdate(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> new JsonObject(getResponse.getSourceAsString()))
                .flatMap(jsonObject -> {
                    Calendar past = getInstance();
                    past.add(MILLISECOND, -(CONSISTENCY_THRESHOLD * 2));

                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    IndexRequestBuilder request = elasticsearch.get()
                            .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    jsonObject.put("update_ts", toDateTimeString(past));
                    request.setSource(jsonObject.encode());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultIndexTimeout())
                            .map(indexResponseOptional -> jsonObject);
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceClusterInfoRefresh(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // First time through the blobs won't be acked so assert that they're not.
                    // The blobs aren't acked the first time since they are modified as part
                    // of a bulk index update which can fail for any number of reasons. If we
                    // ack the blobs as part of this index update it becomes possible for the
                    // associated volume to be acked but not be referenced in the index which
                    // cause the block ranges allocated in the volume to forever remain allocated
                    // or a least until we write some code that scans the volumes and checks
                    // if the object has a reference to it's block range
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // second time through the blobs will be acked so assert that they are
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 1, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .flatMap(getResponse -> {
                    JsonObject jsonObject = new JsonObject(getResponse.getSourceAsMap());
                    for (Object oVersion : jsonObject.getJsonArray("versions")) {
                        JsonObject version = (JsonObject) oVersion;
                        for (Object oSegment : version.getJsonArray("segments")) {
                            JsonObject segment = (JsonObject) oSegment;
                            for (Object oBlob : segment.getJsonArray("blobs")) {
                                JsonObject blob = (JsonObject) oBlob;
                                boolean isReplica = blob.getBoolean("volume_replica");
                                if (isReplica) {
                                    // corrupt all the checksums on the replicas
                                    blob.put("read_sha512", new byte[0]);
                                }
                            }
                        }
                    }
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    IndexRequestBuilder request = elasticsearch.get()
                            .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), getResponse.getId())
                            .setVersion(getResponse.getVersion())
                            .setSource(jsonObject.encode());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultIndexTimeout())
                            .map(Optional::get);
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // total number of blobs will be one primary + 4 replicas that were marked deleted
                    // but not removed from the index. Next time the job runs they will be removed from the index.
                    // This is again done so that we don't end up with orphaned block ranges in the volumes
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 3, totalBlobs.size());
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                // run the job 4 times so the verify/ack retry count expires
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // total number of blobs will be one primary + 4 replicas that were marked deleted
                    // but not removed from the index. Next time the job runs they will be removed from the index.
                    // This is again done so that we don't end up with orphaned block ranges in the volumes
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 2, totalBlobs.size());
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 1, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testFixCorruptPrimary(TestContext context) {
        byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
        getCurrentInstance().nextBytes(data0);
        Async async = context.async();
        prepareContainer(context)

                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().jobs().stop(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // confirm this are in the state they should be before
                    // we fiddle with the replica count
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(segment.verifiedAckdReplicaBlobs()));
                        assertEquals(context, 0, size(segment.verifiedReplicaBlobs()));
                    }
                    return persistentObject;
                })
                .map(aVoid -> {
                    Nodes nodes = VERTX_CONTEXT.verticle().nodes();
                    nodes.setNumberOfReplicas(1)
                            .setAllowSameNode(true);
                    return (Void) null;
                })
                .flatMap(aVoid -> {
                    // create 4 replica volumes
                    VolumeManager volumeManager = VERTX_CONTEXT.verticle().nodes().volumeManager();
                    return volumeManager.newVolume(VERTX_CONTEXT)
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()))
                            .flatMap(aVoid1 -> volumeManager.newVolume(VERTX_CONTEXT))
                            .map(volumeManager::get)
                            .map(Optional::get)
                            .flatMap(volume -> volume.setReplica(VERTX_CONTEXT.vertx()));


                })
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceNodeStatsUpdate(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> new JsonObject(getResponse.getSourceAsString()))
                .flatMap(jsonObject -> {
                    Calendar past = getInstance();
                    past.add(MILLISECOND, -(CONSISTENCY_THRESHOLD * 2));

                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    IndexRequestBuilder request = elasticsearch.get()
                            .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    jsonObject.put("update_ts", toDateTimeString(past));
                    request.setSource(jsonObject.encode());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultIndexTimeout())
                            .map(indexResponseOptional -> jsonObject);
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().nodes().forceClusterInfoRefresh(VERTX_CONTEXT))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // First time through the blobs won't be acked so assert that they're not.
                    // The blobs aren't acked the first time since they are modified as part
                    // of a bulk index update which can fail for any number of reasons. If we
                    // ack the blobs as part of this index update it becomes possible for the
                    // associated volume to be acked but not be referenced in the index which
                    // cause the block ranges allocated in the volume to forever remain allocated
                    // or a least until we write some code that scans the volumes and checks
                    // if the object has a reference to it's block range
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 0, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // second time through the blobs will be acked so assert that they are
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 1, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .flatMap(getResponse -> {
                    JsonObject jsonObject = new JsonObject(getResponse.getSourceAsMap());
                    for (Object oVersion : jsonObject.getJsonArray("versions")) {
                        JsonObject version = (JsonObject) oVersion;
                        for (Object oSegment : version.getJsonArray("segments")) {
                            JsonObject segment = (JsonObject) oSegment;
                            for (Object oBlob : segment.getJsonArray("blobs")) {
                                JsonObject blob = (JsonObject) oBlob;
                                boolean isPrimary = blob.getBoolean("volume_primary");
                                if (isPrimary) {
                                    // corrupt all the checksums on the replicas
                                    blob.put("read_sha512", new byte[0]);
                                }
                            }
                        }
                    }
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    IndexRequestBuilder request = elasticsearch.get()
                            .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), getResponse.getId())
                            .setVersion(getResponse.getVersion())
                            .setSource(jsonObject.encode());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultIndexTimeout())
                            .map(Optional::get);
                })
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // total number of blobs will be one primary + 4 replicas that were marked deleted
                    // but not removed from the index. Next time the job runs they will be removed from the index.
                    // This is again done so that we don't end up with orphaned block ranges in the volumes
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 3, totalBlobs.size());
                        assertEquals(context, 0, size(primaryBlobReferences));
                        assertEquals(context, 1, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                // run the job 4 times so the verify/ack retry count expires
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunJobs(VERTX_CONTEXT))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    GetRequestBuilder request = elasticsearch.get()
                            .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultGetTimeout())
                            .map(Optional::get);
                })
                .map(getResponse -> fromGetResponse(null, getResponse))
                .map(persistentObject -> {
                    // total number of blobs will be one primary + 4 replicas that were marked deleted
                    // but not removed from the index. Next time the job runs they will be removed from the index.
                    // This is again done so that we don't end up with orphaned block ranges in the volumes
                    for (TransientVersion version : persistentObject.getVersions()) {
                        NavigableSet<TransientSegment> segments = version.getSegments();
                        assertEquals(context, 1, segments.size());
                        TransientSegment segment = segments.first();
                        List<TransientBlobReference> totalBlobs = segment.getBlobs();
                        Iterable<TransientBlobReference> primaryBlobReferences = segment.verifiedAckdPrimaryBlobs();
                        Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdReplicaBlobs();
                        assertEquals(context, 2, totalBlobs.size());
                        assertEquals(context, 1, size(primaryBlobReferences));
                        assertEquals(context, 1, size(replicaBlobReferences));
                    }
                    return persistentObject;
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }
}