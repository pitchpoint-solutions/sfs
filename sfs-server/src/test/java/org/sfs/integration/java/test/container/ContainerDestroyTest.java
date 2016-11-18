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

import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.filesystem.volume.VolumeV1;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.DestroyContainer;
import org.sfs.integration.java.func.GetContainer;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.integration.java.func.WaitForCluster;
import org.sfs.rx.BufferToJsonArray;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;
import org.sfs.util.PrngRandom;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.VertxAssert.assertEquals;
import static rx.Observable.just;

public class ContainerDestroyTest extends BaseTestVerticle {


    private final String accountName = "testaccount";
    private final String containerName = "testcontainer";
    private final String objectName = "testobject";

    private Producer authAdmin = httpBasic("admin", "admin");
    private Producer authNonAdmin = httpBasic("user", "user");


    protected Observable<Void> prepareContainer(TestContext context) {

        return just((Void) null)
                .flatMap(aVoid -> vertxContext.verticle().getNodeStats().forceUpdate(vertxContext))
                .flatMap(aVoid -> vertxContext.verticle().getClusterInfo().forceRefresh(vertxContext))
                .flatMap(new WaitForCluster(vertxContext))
                .flatMap(new PostAccount(httpClient, accountName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutContainer(httpClient, accountName, containerName, authNonAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .count()
                .map(new ToVoid<Integer>());
    }

    @Test
    public void testDestroyContainerWithSmallObjects(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO1".getBytes(UTF_8);
        final byte[] data2 = "HELLO2".getBytes(UTF_8);

        Async async = context.async();
        prepareContainer(context)

                // put three objects then list and assert
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/4", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/3", authNonAdmin, data2)
                        .setHeader(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString()))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/2", authNonAdmin, data1))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/1", authNonAdmin, data0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(httpClient, authAdmin))
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonArray())
                .map(new Func1<JsonArray, Void>() {
                    @Override
                    public Void call(JsonArray jsonArray) {
                        assertEquals(context, 4, jsonArray.size());
                        return null;
                    }
                })
                .flatMap(new DestroyContainer(httpClient, accountName, containerName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(httpClient, authAdmin))
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testDestroyContainerWithLargeObjects(TestContext context) {
        final byte[] data0 = new byte[VolumeV1.TINY_DATA_THRESHOLD * 2];
        final byte[] data1 = new byte[VolumeV1.TINY_DATA_THRESHOLD * 2];
        final byte[] data2 = new byte[VolumeV1.TINY_DATA_THRESHOLD * 2];

        PrngRandom.getCurrentInstance().nextBytesBlocking(data0);
        PrngRandom.getCurrentInstance().nextBytesBlocking(data1);
        PrngRandom.getCurrentInstance().nextBytesBlocking(data2);

        Async async = context.async();
        prepareContainer(context)

                // put three objects then list and assert
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/4", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/3", authNonAdmin, data2)
                        .setHeader(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString()))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/2", authNonAdmin, data1))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/1", authNonAdmin, data0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(httpClient, authAdmin))
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonArray())
                .map(new Func1<JsonArray, Void>() {
                    @Override
                    public Void call(JsonArray jsonArray) {
                        assertEquals(context, 4, jsonArray.size());
                        return null;
                    }
                })
                .flatMap(new DestroyContainer(httpClient, accountName, containerName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(httpClient, authAdmin))
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NOT_FOUND))
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

}
