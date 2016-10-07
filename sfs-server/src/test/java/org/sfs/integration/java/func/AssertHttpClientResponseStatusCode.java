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

package org.sfs.integration.java.func;

import io.vertx.core.http.HttpClientResponse;
import io.vertx.ext.unit.TestContext;
import rx.functions.Func1;

import static org.sfs.util.VertxAssert.assertEquals;

public class AssertHttpClientResponseStatusCode implements Func1<HttpClientResponse, HttpClientResponse> {

    private final TestContext testContext;
    private final int expectedStatus;

    public AssertHttpClientResponseStatusCode(TestContext testContext, int expectedStatus) {
        this.testContext = testContext;
        this.expectedStatus = expectedStatus;
    }

    @Override
    public HttpClientResponse call(HttpClientResponse httpClientResponse) {
        assertEquals(testContext, expectedStatus, httpClientResponse.statusCode());
        return httpClientResponse;
    }
}
