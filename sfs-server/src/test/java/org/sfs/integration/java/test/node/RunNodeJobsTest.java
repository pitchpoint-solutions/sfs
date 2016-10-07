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

package org.sfs.integration.java.test.node;

import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PostContainer;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.integration.java.func.RunNodeJobs;
import org.sfs.rx.BufferToJsonObject;
import org.sfs.rx.HttpClientKeepAliveResponseBodyBuffer;
import org.sfs.rx.ToType;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.nio.file.Path;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.file.Files.createTempDirectory;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.KnownMetadataKeys.X_MAX_OBJECT_REVISIONS;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META_PREFIX;
import static org.sfs.util.SfsHttpHeaders.X_SERVER_SIDE_ENCRYPTION;
import static org.sfs.util.VertxAssert.assertEquals;
import static rx.Observable.just;

public class RunNodeJobsTest extends BaseTestVerticle {

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
    public void test(TestContext context) throws IOException {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final long currentTimeInMillis = currentTimeMillis();

        Path tempDirectory = createTempDirectory("");

        Async async = context.async();
        prepareContainer(context)
                .flatMap(new PostContainer(HTTP_CLIENT, accountName, containerName, authAdmin)
                        .setHeader(X_ADD_CONTAINER_META_PREFIX + X_MAX_OBJECT_REVISIONS, valueOf(30)))
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                // put an object then get/head the object
                .flatMap(new PutObject(HTTP_CLIENT, accountName, containerName, objectName, authNonAdmin, data0)
                        .setHeader(X_SERVER_SIDE_ENCRYPTION, valueOf(true)))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new RunNodeJobs(HTTP_CLIENT, authAdmin, tempDirectory))
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(new Func1<JsonObject, JsonObject>() {
                    @Override
                    public JsonObject call(JsonObject jsonObject) {
                        assertEquals(context, HTTP_OK, jsonObject.getInteger("code").intValue());
                        assertEquals(context, "Success", jsonObject.getString("message"));
                        return jsonObject;
                    }
                })
                .map(new ToType<>((Void) null))
                .subscribe(new TestSubscriber(context, async));
    }

}
