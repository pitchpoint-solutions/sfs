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

package org.sfs.integration.java.test.account;

import io.vertx.core.http.HttpClientResponse;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.GetAccount;
import org.sfs.integration.java.func.HeadAccount;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static rx.Observable.just;

public class AccountPermissionsTest extends BaseTestVerticle {

    private Producer authAdmin = httpBasic("admin", "admin");

    @Test
    public void testNoPermissions(TestContext context) {
        runOnServerContext(context, () -> {
            final String accountName = "testaccount";

            return just((Void) null)
                    .flatMap(new Func1<Void, Observable<Void>>() {
                        @Override
                        public Observable<Void> call(Void aVoid) {
                            return testPermission(context, accountName, "", "");
                        }
                    });
        });
    }

    @Test
    public void testAllButAdminPermission(TestContext context) {
        runOnServerContext(context, () -> {
            final String accountName = "testaccount";


            return just((Void) null)
                    .flatMap(new Func1<Void, Observable<Void>>() {
                        @Override
                        public Observable<Void> call(Void aVoid) {
                            return testPermission(context, accountName, "user", "user");
                        }
                    });
        });
    }

    protected Observable<Void> testPermission(TestContext context, final String accountName, String username, String password) {
        Producer auth = httpBasic(username, password);
        return just((Void) null)
                .flatMap(new PostAccount(httpClient(), accountName, auth))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_FORBIDDEN))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(httpClient(), authAdmin))
                .flatMap(new GetAccount(httpClient(), accountName, auth)
                        .setMediaTypes(JSON_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, "".equals(username) ? HTTP_FORBIDDEN : HTTP_NOT_FOUND))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new HeadAccount(httpClient(), accountName, auth))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_FORBIDDEN))
                .map(new ToVoid<HttpClientResponse>());
    }
}
