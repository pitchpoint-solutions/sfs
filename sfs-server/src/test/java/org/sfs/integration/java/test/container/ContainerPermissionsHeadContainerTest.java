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
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.HeadContainer;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpClientResponseHeaderLogger;

import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static rx.Observable.just;

public class ContainerPermissionsHeadContainerTest extends BaseTestVerticle {

    private final String accountName = "testaccount";
    private final String containerName = "testcontainer";
    private Producer authAdmin = httpBasic("admin", "admin");

    @Test
    public void testHeadContainerImplicitAllow(TestContext context) {
        runOnServerContext(context, () -> {
            Producer authNonAdmin0 = httpBasic("user", "user");

            return just((Void) null)
                    .flatMap(new PostAccount(httpClient(), accountName, authAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, containerName, authNonAdmin0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new HeadContainer(httpClient(), accountName, containerName, authNonAdmin0))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>());
        });
    }

}
