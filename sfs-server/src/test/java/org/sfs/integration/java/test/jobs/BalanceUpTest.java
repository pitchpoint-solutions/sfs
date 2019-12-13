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
import io.vertx.ext.unit.TestContext;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.junit.Test;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.integration.java.func.UpdateClusterStats;
import org.sfs.integration.java.func.VerifyRepairAllContainersExecute;
import org.sfs.jobs.VerifyRepairAllContainerObjects;
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
import static java.util.Calendar.getInstance;
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
                .flatMap(new PostAccount(httpClient(), accountName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<>())
                .flatMap(new PutContainer(httpClient(), accountName, containerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<>())
                .count()
                .map(new ToVoid<>());
    }

    @Test
    public void testIncreaseDecreaseNumberOfReplicas(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);
            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(aVoid -> vertxContext().verticle().jobs().close(vertxContext()))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
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
                            assertEquals(context, 1, size(segment.verifiedAckdBlobs()));
                            assertEquals(context, 0, size(segment.verifiedUnAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(aVoid -> {
                        Nodes nodes = vertxContext().verticle().nodes();
                        nodes.setNumberOfObjectReplicas(3)
                                .setAllowSameNode(true);
                        return (Void) null;
                    })
                    .flatMap(aVoid -> {
                        // create 4 replica volumes
                        VolumeManager volumeManager = vertxContext().verticle().nodes().volumeManager();
                        return volumeManager.newVolume(vertxContext())
                                .flatMap(aVoid1 -> volumeManager.newVolume(vertxContext()))
                                .flatMap(aVoid1 -> volumeManager.newVolume(vertxContext()))
                                .flatMap(aVoid1 -> volumeManager.newVolume(vertxContext()));


                    })
                    .map(new ToVoid<>())
                    .flatMap(new UpdateClusterStats(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    })
                    .map(getResponse -> new JsonObject(getResponse.getSourceAsString()))
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
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new UpdateClusterStats(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
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
                            assertEquals(context, 1, size(segment.verifiedAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(new ToVoid<>())
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
                    .map(getResponse -> fromGetResponse(null, getResponse))
                    .map(persistentObject -> {
                        // second time through the blobs will be acked so assert that they are
                        for (TransientVersion version : persistentObject.getVersions()) {
                            NavigableSet<TransientSegment> segments = version.getSegments();
                            assertEquals(context, 1, segments.size());
                            TransientSegment segment = segments.first();
                            assertEquals(context, 4, size(segment.verifiedAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(new ToVoid<>())
                    .map(aVoid -> {
                        Nodes nodes = vertxContext().verticle().nodes();
                        nodes.setNumberOfObjectReplicas(0)
                                .setAllowSameNode(true);
                        return (Void) null;
                    })
                    .flatMap(new UpdateClusterStats(httpClient(), authAdmin))
                    .map(new ToVoid<>())
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
                            assertEquals(context, 4, totalBlobs.size());
                            assertEquals(context, 1, size(segment.verifiedAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(new ToVoid<>())
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
                            assertEquals(context, 1, totalBlobs.size());
                            assertEquals(context, 1, size(segment.verifiedAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(new ToVoid<>());
        });
    }

    @Test
    public void testFixCorruptReplica(TestContext context) {
        runOnServerContext(context, () -> {
            byte[] data0 = new byte[TINY_DATA_THRESHOLD + 1];
            getCurrentInstance().nextBytesBlocking(data0);
            return prepareContainer(context)

                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName, objectName, authNonAdmin, data0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(aVoid -> vertxContext().verticle().jobs().close(vertxContext()))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
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
                            assertEquals(context, 1, size(segment.verifiedAckdBlobs()));
                            assertEquals(context, 0, size(segment.verifiedUnAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(aVoid -> {
                        Nodes nodes = vertxContext().verticle().nodes();
                        nodes.setNumberOfObjectReplicas(3)
                                .setAllowSameNode(true);
                        return (Void) null;
                    })
                    .flatMap(aVoid -> {
                        // create 4 replica volumes
                        VolumeManager volumeManager = vertxContext().verticle().nodes().volumeManager();
                        return volumeManager.newVolume(vertxContext())
                                .flatMap(aVoid1 -> volumeManager.newVolume(vertxContext()))
                                .flatMap(aVoid1 -> volumeManager.newVolume(vertxContext()))
                                .flatMap(aVoid1 -> volumeManager.newVolume(vertxContext()));


                    })
                    .map(new ToVoid<>())
                    .flatMap(new UpdateClusterStats(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    })
                    .map(getResponse -> new JsonObject(getResponse.getSourceAsString()))
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
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new UpdateClusterStats(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
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
                            assertEquals(context, 1, size(segment.verifiedAckdBlobs()));
                            assertEquals(context, 3, size(segment.verifiedUnAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(new ToVoid<>())
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
                    .map(getResponse -> fromGetResponse(null, getResponse))
                    .map(persistentObject -> {
                        // second time through the blobs will be acked so assert that they are
                        for (TransientVersion version : persistentObject.getVersions()) {
                            NavigableSet<TransientSegment> segments = version.getSegments();
                            assertEquals(context, 1, segments.size());
                            TransientSegment segment = segments.first();
                            Iterable<TransientBlobReference> replicaBlobReferences = segment.verifiedAckdBlobs();
                            assertEquals(context, 4, size(replicaBlobReferences));
                        }
                        return persistentObject;
                    })
                    .flatMap(aVoid -> {
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        GetRequestBuilder request = elasticsearch.get()
                                .prepareGet(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), fromPaths(accountName, containerName, objectName).objectPath().get());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultGetTimeout())
                                .map(Optional::get);
                    })
                    .flatMap(getResponse -> {
                        JsonObject jsonObject = new JsonObject(getResponse.getSourceAsMap());
                        for (Object oVersion : jsonObject.getJsonArray("versions")) {
                            JsonObject version = (JsonObject) oVersion;
                            for (Object oSegment : version.getJsonArray("segments")) {
                                JsonObject segment = (JsonObject) oSegment;
                                boolean first = true;
                                for (Object oBlob : segment.getJsonArray("blobs")) {
                                    JsonObject blob = (JsonObject) oBlob;
                                    if (!first) {
                                        // corrupt the checksums for all but one
                                        blob.put("read_sha512", new byte[0]);
                                    } else {
                                        first = false;
                                    }
                                }
                            }
                        }
                        Elasticsearch elasticsearch = vertxContext().verticle().elasticsearch();
                        IndexRequestBuilder request = elasticsearch.get()
                                .prepareIndex(elasticsearch.objectIndex(containerName), elasticsearch.defaultType(), getResponse.getId())
                                .setVersion(getResponse.getVersion())
                                .setSource(jsonObject.encode());
                        return elasticsearch.execute(vertxContext(), request, elasticsearch.getDefaultIndexTimeout())
                                .map(Optional::get);
                    })
                    .map(new ToVoid<>())
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
                            assertEquals(context, 7, totalBlobs.size());
                            assertEquals(context, 1, size(segment.verifiedAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(new ToVoid<>())
                    // run the job 4 times so the verify/ack retry count expires
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new VerifyRepairAllContainersExecute(httpClient(), authAdmin))
                    .map(new ToVoid<>())
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
                            assertEquals(context, 4, totalBlobs.size());
                            assertEquals(context, 4, size(segment.verifiedAckdBlobs()));
                        }
                        return persistentObject;
                    })
                    .map(new ToVoid<>());
        });
    }
}