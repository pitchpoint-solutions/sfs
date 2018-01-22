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

import com.google.common.collect.ListMultimap;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.HeadContainer;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PostContainer;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.collect.ArrayListMultimap.create;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.NullSafeAscii.toLowerCase;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META_PREFIX;
import static org.sfs.util.SfsHttpHeaders.X_REMOVE_CONTAINER_META_PREFIX;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertFalse;
import static rx.Observable.just;

public class ContainerMetadataTest extends BaseTestVerticle {

    @Test
    public void testPutNoMetata(TestContext context) {
        runOnServerContext(context, () -> {
            final String accountName = "testaccount";
            final String containerName = "testcontainer";
            ListMultimap<String, String> headers = create();


            Producer auth = httpBasic("admin", "admin");

            return just((Void) null)
                    .flatMap(new PostAccount(httpClient(), accountName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, containerName, auth, headers))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new HeadContainer(httpClient(), accountName, containerName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                        @Override
                        public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                            MultiMap headers = httpClientResponse.headers();
                            for (String headerName : headers.names()) {
                                headerName = toLowerCase(headerName);
                                assertFalse(context, headerName.startsWith(X_ADD_CONTAINER_META_PREFIX));
                            }
                            return httpClientResponse;
                        }
                    })
                    .count()
                    .map(new Func1<Integer, Void>() {
                        @Override
                        public Void call(Integer integer) {
                            return null;
                        }
                    });
        });
    }

    @Test
    public void testPutOneMetadata(TestContext context) {
        runOnServerContext(context, () -> {
            final String accountName = "testaccount";
            final String containerName = "testcontainer";

            final String selectedHeaderName = format("%s%s", X_ADD_CONTAINER_META_PREFIX, "TEST_DATA");

            final ListMultimap<String, String> headers = create();
            headers.put(selectedHeaderName, "TEST_VALUE");

            Producer auth = httpBasic("admin", "admin");

            return just((Void) null)
                    .flatMap(new PostAccount(httpClient(), accountName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, containerName, auth, headers))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new HeadContainer(httpClient(), accountName, containerName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                        @Override
                        public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                            MultiMap headers = httpClientResponse.headers();
                            int count = 0;
                            for (String headerName : headers.names()) {
                                headerName = toLowerCase(headerName);
                                String prefix = toLowerCase(X_ADD_CONTAINER_META_PREFIX);
                                if (headerName.startsWith(prefix)) {
                                    count++;
                                }
                            }
                            assertEquals(context, 1, count);
                            assertEquals(context, "TEST_VALUE", headers.get(selectedHeaderName));
                            return httpClientResponse;
                        }
                    })
                    .map(new ToVoid<HttpClientResponse>());
        });
    }

    @Test
    public void testPutTwoMetadataRemoveOneMetadata(TestContext context) {
        runOnServerContext(context, () -> {
            final String accountName = "testaccount";
            final String containerName = "testcontainer";

            final String selectedHeaderName0 = format("%s%s", X_ADD_CONTAINER_META_PREFIX, "TEST_DATA0");
            final String selectedHeaderName1 = format("%s%s", X_ADD_CONTAINER_META_PREFIX, "TEST_DATA1");

            final ListMultimap<String, String> headers = create();
            headers.put(selectedHeaderName0, "TEST_VALUE0");
            headers.put(selectedHeaderName1, "TEST_VALUE1");

            final Producer auth = httpBasic("admin", "admin");

            return just((Void) null)
                    .flatMap(new PostAccount(httpClient(), accountName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, containerName, auth, headers))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new HeadContainer(httpClient(), accountName, containerName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                        @Override
                        public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                            MultiMap headers = httpClientResponse.headers();
                            int count = 0;
                            for (String headerName : headers.names()) {
                                headerName = toLowerCase(headerName);
                                String prefix = toLowerCase(X_ADD_CONTAINER_META_PREFIX);
                                if (headerName.startsWith(prefix)) {
                                    count++;
                                }
                            }
                            assertEquals(context, 2, count);
                            assertEquals(context, "TEST_VALUE0", headers.get(selectedHeaderName0));
                            assertEquals(context, "TEST_VALUE1", headers.get(selectedHeaderName1));
                            return httpClientResponse;
                        }
                    })
                    .flatMap(new Func1<HttpClientResponse, Observable<HttpClientResponse>>() {
                        @Override
                        public Observable<HttpClientResponse> call(HttpClientResponse httpClientResponse) {
                            final String name = format("%s%s", X_REMOVE_CONTAINER_META_PREFIX, "TEST_DATA1");
                            final ListMultimap<String, String> map = create();
                            map.put(name, "TEST_VALUE0");
                            return just((Void) null)
                                    .flatMap(new PostContainer(httpClient(), accountName, containerName, auth, map));
                        }
                    })
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new HeadContainer(httpClient(), accountName, containerName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new Func1<HttpClientResponse, HttpClientResponse>() {
                        @Override
                        public HttpClientResponse call(HttpClientResponse httpClientResponse) {
                            MultiMap headers = httpClientResponse.headers();
                            int count = 0;
                            for (String headerName : headers.names()) {
                                headerName = toLowerCase(headerName);
                                String prefix = toLowerCase(X_ADD_CONTAINER_META_PREFIX);
                                if (headerName.startsWith(prefix)) {
                                    count++;
                                }
                            }
                            assertEquals(context, 1, count);
                            assertEquals(context, "TEST_VALUE0", headers.get(selectedHeaderName0));
                            return httpClientResponse;
                        }
                    })
                    .map(new ToVoid<HttpClientResponse>());
        });
    }

}


