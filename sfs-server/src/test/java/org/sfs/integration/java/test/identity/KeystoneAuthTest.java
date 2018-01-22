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

package org.sfs.integration.java.test.identity;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.KeystoneAuth;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static rx.Observable.just;

public class KeystoneAuthTest extends BaseTestVerticle {


    @Test
    public void testValidCreds(TestContext context) {
        runOnServerContext(context, () -> {
            return just((Void) null)
                    .flatMap(new KeystoneAuth(httpClient(), "user", "user"))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new ToVoid<Buffer>());
        });
    }

    @Test
    public void testInvalid(TestContext context) {
        runOnServerContext(context, () -> {
            return just((Void) null)
                    .flatMap(new KeystoneAuth(httpClient(), "user", "badpassword"))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_UNAUTHORIZED))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new ToVoid<Buffer>());
        });
    }
}
