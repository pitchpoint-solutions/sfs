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

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.GetAccount;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.PutObject;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.rx.BufferToDom;
import org.sfs.rx.BufferToJsonArray;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import rx.Observable;
import rx.functions.Func1;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.APPLICATION_XML_UTF_8;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.SfsHttpQueryParams.END_MARKER;
import static org.sfs.util.SfsHttpQueryParams.LIMIT;
import static org.sfs.util.SfsHttpQueryParams.MARKER;
import static org.sfs.util.SfsHttpQueryParams.PREFIX;
import static org.sfs.util.VertxAssert.assertEquals;
import static rx.Observable.just;


public class AccountListingTest extends BaseTestVerticle {

    private static final Logger LOGGER = getLogger(AccountListingTest.class);
    private final String accountName = "testaccount";
    private final String containerName1 = "testcontainer1";
    private final String containerName2 = "testcontainer2";
    private final String objectName = "testobject";

    private Producer authAdmin = httpBasic("admin", "admin");
    private Producer authNonAdmin = httpBasic("user", "user");


    public AccountListingTest() {
    }

    protected Observable<Void> prepareAccount(TestContext context) {
        return just((Void) null)
                .flatMap(new PostAccount(httpClient(), accountName, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .count()
                .map(new ToVoid<Integer>());
    }

    @Test
    public void testListAccountObjects(TestContext context) {
        runOnServerContext(context, () -> {
            final byte[] data0 = "HELLO0".getBytes(UTF_8);
            final byte[] data1 = "HELLO1".getBytes(UTF_8);
            final byte[] data2 = "HELLO2".getBytes(UTF_8);

            return prepareAccount(context)
                    .flatMap(new PutContainer(httpClient(), accountName, containerName1, authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, containerName2, authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    // put three objects then list and assert
                    .flatMap(new PutObject(httpClient(), accountName, containerName1, objectName + "/4", authNonAdmin, new byte[]{}))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName1, objectName + "/3", authNonAdmin, data2)
                            .setHeader(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString()))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName1, objectName + "/2", authNonAdmin, data1))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName1, objectName + "/1", authNonAdmin, data0))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName2, objectName + "/4", authNonAdmin, new byte[]{}))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName2, objectName + "/3", authNonAdmin, data2)
                            .setHeader(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString()))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutObject(httpClient(), accountName, containerName2, objectName + "/2", authNonAdmin, data1))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setMediaTypes(JSON_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new BufferToJsonArray())
                    .map(new Func1<JsonArray, Void>() {
                        @Override
                        public Void call(JsonArray jsonArray) {
                            assertEquals(context, 2, jsonArray.size());
                            for (Object o : jsonArray) {
                                JsonObject jsonObject = (JsonObject) o;
                                String name = jsonObject.getString("name");
                                int count = jsonObject.getInteger("count");
                                int bytes = jsonObject.getInteger("bytes");

                                switch (name) {
                                    case containerName1:
                                        assertEquals(context, containerName1, name);
                                        assertEquals(context, 4 /** 3 non zero length + a zero length object */, count);
                                        assertEquals(context, data0.length + data1.length + data2.length, bytes);
                                        break;
                                    case containerName2:
                                        assertEquals(context, containerName2, name);
                                        assertEquals(context, 3 /** 3 non zero length objects */, count);
                                        assertEquals(context, data1.length + data2.length, bytes);
                                        break;
                                    default:
                                        context.fail();
                                }
                            }
                            return null;
                        }
                    })
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setMediaTypes(APPLICATION_XML_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new BufferToDom())
                    .map(new Func1<Document, Void>() {
                        @Override
                        public Void call(Document document) {
                            assertEquals(context, 2, document.getElementsByTagName("container").getLength());

                            assertEquals(context, "account", document.getDocumentElement().getNodeName());
                            assertEquals(context, accountName, document.getDocumentElement().getAttribute("name"));

                            NodeList childNodes = document.getDocumentElement().getChildNodes();
                            assertEquals(context, 2, childNodes.getLength());

                            for (int i = 0; i < childNodes.getLength(); i++) {
                                Element childNode = (Element) childNodes.item(i);

                                assertEquals(context, "container", childNode.getNodeName());

                                NodeList elementsOfChild = childNode.getChildNodes();

                                String name = null;
                                Long count = null;
                                Long bytes = null;

                                for (int j = 0; j < elementsOfChild.getLength(); j++) {
                                    Node childElement = elementsOfChild.item(j);
                                    String localName = childElement.getNodeName();
                                    if ("name".equals(localName)) {
                                        name = childElement.getTextContent();
                                    } else if ("count".equals(localName)) {
                                        count = parseLong(childElement.getTextContent());
                                    } else if ("bytes".equals(localName)) {
                                        bytes = parseLong(childElement.getTextContent());
                                    }
                                }

                                switch (name) {
                                    case containerName1:
                                        assertEquals(context, containerName1, name);
                                        assertEquals(context, 4 /** 3 non zero length + a zero length object */, count.intValue());
                                        assertEquals(context, data0.length + data1.length + data2.length, bytes.intValue());
                                        break;
                                    case containerName2:
                                        assertEquals(context, containerName2, name);
                                        assertEquals(context, 3 /** 3 non zero length objects */, count.intValue());
                                        assertEquals(context, data1.length + data2.length, bytes.intValue());
                                        break;
                                    default:
                                        context.fail();
                                }

                            }
                            return null;
                        }
                    })
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setMediaTypes(PLAIN_TEXT_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new Func1<Buffer, Void>() {
                        @Override
                        public Void call(Buffer buffer) {
                            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer.getBytes()), UTF_8))) {
                                String line;
                                while ((line = bufferedReader.readLine()) != null) {
                                    switch (line) {
                                        case containerName1:
                                            break;
                                        case containerName2:
                                            break;
                                        default:
                                            context.fail();
                                    }
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        }
                    });
        });
    }

    @Test
    public void testListAccountQueryParamsObjects(TestContext context) {
        runOnServerContext(context, () -> {
            return prepareAccount(context)
                    .flatMap(new PutContainer(httpClient(), accountName, "apples", authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, "apricots", authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, "bananas", authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, "kiwis", authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, "oranges", authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new PutContainer(httpClient(), accountName, "pears", authNonAdmin))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setQueryParam(LIMIT, "2")
                            .setMediaTypes(JSON_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new BufferToJsonArray())
                    .map(new Func1<JsonArray, Void>() {
                        @Override
                        public Void call(JsonArray jsonArray) {
                            assertEquals(context, 2, jsonArray.size());
                            return null;
                        }
                    })
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setQueryParam(MARKER, "bananas")
                            .setMediaTypes(JSON_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new BufferToJsonArray())
                    .map(new Func1<JsonArray, Void>() {
                        @Override
                        public Void call(JsonArray jsonArray) {
                            assertEquals(context, 3, jsonArray.size());

                            for (Object o : jsonArray) {
                                JsonObject jsonObject = (JsonObject) o;
                                String name = jsonObject.getString("name");

                                switch (name) {
                                    case "kiwis":
                                        break;
                                    case "oranges":
                                        break;
                                    case "pears":
                                        break;
                                    default:
                                        context.fail();
                                }
                            }

                            return null;
                        }
                    })
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setQueryParam(END_MARKER, "oranges")
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

                            for (Object o : jsonArray) {
                                JsonObject jsonObject = (JsonObject) o;
                                String name = jsonObject.getString("name");

                                switch (name) {
                                    case "apples":
                                        break;
                                    case "apricots":
                                        break;
                                    case "bananas":
                                        break;
                                    case "kiwis":
                                        break;
                                    default:
                                        context.fail();
                                }
                            }

                            return null;
                        }
                    })
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setQueryParam(MARKER, "bananas")
                            .setQueryParam(END_MARKER, "oranges")
                            .setMediaTypes(JSON_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new BufferToJsonArray())
                    .map(new Func1<JsonArray, Void>() {
                        @Override
                        public Void call(JsonArray jsonArray) {
                            assertEquals(context, 1, jsonArray.size());

                            for (Object o : jsonArray) {
                                JsonObject jsonObject = (JsonObject) o;
                                String name = jsonObject.getString("name");

                                switch (name) {
                                    case "kiwis":
                                        break;
                                    default:
                                        context.fail();
                                }
                            }

                            return null;
                        }
                    })
                    .flatMap(new GetAccount(httpClient(), accountName, authAdmin)
                            .setQueryParam(PREFIX, "ap")
                            .setMediaTypes(JSON_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new BufferToJsonArray())
                    .map(new Func1<JsonArray, Void>() {
                        @Override
                        public Void call(JsonArray jsonArray) {
                            assertEquals(context, 2, jsonArray.size());

                            for (Object o : jsonArray) {
                                JsonObject jsonObject = (JsonObject) o;
                                String name = jsonObject.getString("name");

                                switch (name) {
                                    case "apples":
                                        break;
                                    case "apricots":
                                        break;
                                    default:
                                        context.fail();
                                }
                            }

                            return null;
                        }
                    });
        });
    }

    @Test
    public void testPutListEmptyAccount(TestContext context) {
        runOnServerContext(context, () -> {
            final String accountName = "testaccount";

            Producer auth = httpBasic("admin", "admin");

            return just((Void) null)
                    .flatMap(new PostAccount(httpClient(), accountName, auth))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                    .map(new ToVoid<HttpClientResponse>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new GetAccount(httpClient(), accountName, auth)
                            .setMediaTypes(JSON_UTF_8))
                    .map(new HttpClientResponseHeaderLogger())
                    .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                    .flatMap(new HttpClientResponseBodyBuffer())
                    .map(new HttpBodyLogger())
                    .map(new BufferToJsonArray())
                    .map(new Func1<JsonArray, Void>() {
                        @Override
                        public Void call(JsonArray jsonObject) {
                            assertEquals(context, 0, jsonObject.size());
                            return null;
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

}
