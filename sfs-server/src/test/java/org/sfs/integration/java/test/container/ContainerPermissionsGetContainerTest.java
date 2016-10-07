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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.GetContainer;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static rx.Observable.just;

public class ContainerPermissionsGetContainerTest extends BaseTestVerticle {

    private final String accountName = "testaccount";
    private final String containerName = "testcontainer";
    private Producer authAdmin = httpBasic("admin", "admin");

    @Test
    public void testListContainerExplicitAllow(TestContext context) {
        Producer authNonAdmin0 = httpBasic("user", "user");

        Async async = context.async();
        just((Void) null)
                .flatMap(new PostAccount(HTTP_CLIENT, accountName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new GetContainer(HTTP_CLIENT, accountName, containerName, authNonAdmin0))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new ToVoid<Buffer>())
                .subscribe(new TestSubscriber(context, async));
    }

}
