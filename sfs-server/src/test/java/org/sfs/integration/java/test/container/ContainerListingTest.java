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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.GetContainer;
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
import static com.google.common.collect.ArrayListMultimap.create;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.APPLICATION_XML_UTF_8;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.net.MediaType.OCTET_STREAM;
import static com.google.common.net.MediaType.PLAIN_TEXT_UTF_8;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.SfsHttpQueryParams.DELIMITER;
import static org.sfs.util.SfsHttpQueryParams.END_MARKER;
import static org.sfs.util.SfsHttpQueryParams.MARKER;
import static org.sfs.util.SfsHttpQueryParams.PREFIX;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.vo.Segment.EMPTY_MD5;
import static rx.Observable.just;

public class ContainerListingTest extends BaseTestVerticle {


    private final String accountName = "testaccount";
    private final String containerName = "testcontainer";
    private final String objectName = "testobject";

    private Producer authAdmin = httpBasic("admin", "admin");
    private Producer authNonAdmin = httpBasic("user", "user");


    protected Observable<Void> prepareContainer(TestContext context) {

        return just((Void) null)
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
    public void testListContainerObjects(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO1".getBytes(UTF_8);
        final byte[] data2 = "HELLO2".getBytes(UTF_8);

        final String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        final String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());
        final String md52 = base16().lowerCase().encode(md5().hashBytes(data2).asBytes());

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
                        for (Object o : jsonArray) {
                            JsonObject jsonObject = (JsonObject) o;
                            String name = jsonObject.getString("name");
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case objectName + "/4":
                                    assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                case objectName + "/3":
                                    assertEquals(context, md52, hash);
                                    assertEquals(context, PLAIN_TEXT_UTF_8.toString(), mediaType);
                                    assertEquals(context, data2.length, bytes);
                                    break;
                                case objectName + "/2":
                                    assertEquals(context, md51, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data1.length, bytes);
                                    break;
                                case objectName + "/1":
                                    assertEquals(context, md50, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data0.length, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(APPLICATION_XML_UTF_8))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_OK))
                .flatMap(new HttpClientResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToDom())
                .map(new Func1<Document, Void>() {
                    @Override
                    public Void call(Document document) {
                        assertEquals(context, 4, document.getElementsByTagName("object").getLength());

                        assertEquals(context, "container", document.getDocumentElement().getNodeName());
                        assertEquals(context, containerName, document.getDocumentElement().getAttribute("name"));

                        NodeList childNodes = document.getDocumentElement().getChildNodes();
                        assertEquals(context, 4, childNodes.getLength());

                        for (int i = 0; i < childNodes.getLength(); i++) {
                            Element childNode = (Element) childNodes.item(i);

                            assertEquals(context, "object", childNode.getNodeName());

                            NodeList elementsOfChild = childNode.getChildNodes();

                            String name = null;
                            String hash = null;
                            String mediaType = null;
                            Long bytes = null;

                            for (int j = 0; j < elementsOfChild.getLength(); j++) {
                                Node childElement = elementsOfChild.item(j);
                                String localName = childElement.getNodeName();
                                if ("name".equals(localName)) {
                                    name = childElement.getTextContent();
                                } else if ("hash".equals(localName)) {
                                    hash = childElement.getTextContent();
                                } else if ("content_type".equals(localName)) {
                                    mediaType = childElement.getTextContent();
                                } else if ("bytes".equals(localName)) {
                                    bytes = parseLong(childElement.getTextContent());
                                }
                            }

                            switch (name) {
                                case objectName + "/4":
                                    assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, 0, bytes.longValue());
                                    break;
                                case objectName + "/3":
                                    assertEquals(context, md52, hash);
                                    assertEquals(context, PLAIN_TEXT_UTF_8.toString(), mediaType);
                                    assertEquals(context, data2.length, bytes.longValue());
                                    break;
                                case objectName + "/2":
                                    assertEquals(context, md51, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data1.length, bytes.longValue());
                                    break;
                                case objectName + "/1":
                                    assertEquals(context, md50, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data0.length, bytes.longValue());
                                    break;
                                default:
                                    context.fail();
                            }

                        }
                        return null;
                    }
                })
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
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
                                    case objectName + "/4":
                                        break;
                                    case objectName + "/3":
                                        break;
                                    case objectName + "/2":
                                        break;
                                    case objectName + "/1":
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
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testListPrefixObjects(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO11".getBytes(UTF_8);
        final byte[] data2 = "HELLO222".getBytes(UTF_8);

        final String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        final String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());
        final String md52 = base16().lowerCase().encode(md5().hashBytes(data2).asBytes());

        Async async = context.async();
        prepareContainer(context)

                // put three objects then list and assert
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/4", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/3", authNonAdmin, data2)
                        .setHeader(CONTENT_TYPE, PLAIN_TEXT_UTF_8.toString()))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/3/1", authNonAdmin, data1))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, objectName + "/3/2", authNonAdmin, data0))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(httpClient, authAdmin))
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(PREFIX, objectName + "/3/"))
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
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case objectName + "/3/1":
                                    assertEquals(context, md51, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data1.length, bytes);
                                    break;
                                case objectName + "/3/2":
                                    assertEquals(context, md50, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data0.length, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testListMarkerGteObjects(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO11".getBytes(UTF_8);
        final byte[] data2 = "HELLO222".getBytes(UTF_8);

        final String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        final String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());
        final String md52 = base16().lowerCase().encode(md5().hashBytes(data2).asBytes());

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
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(MARKER, objectName + "/2"))
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
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case objectName + "/4":
                                    assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                case objectName + "/3":
                                    assertEquals(context, md52, hash);
                                    assertEquals(context, PLAIN_TEXT_UTF_8.toString(), mediaType);
                                    assertEquals(context, data2.length, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testListMarkerLteObjects(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO11".getBytes(UTF_8);
        final byte[] data2 = "HELLO222".getBytes(UTF_8);

        final String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        final String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());
        final String md52 = base16().lowerCase().encode(md5().hashBytes(data2).asBytes());

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
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(END_MARKER, objectName + "/2"))
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
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case objectName + "/1":
                                    assertEquals(context, md50, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data0.length, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testListMarkerGetAndLteObjects(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO11".getBytes(UTF_8);
        final byte[] data2 = "HELLO222".getBytes(UTF_8);

        final String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        final String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());
        final String md52 = base16().lowerCase().encode(md5().hashBytes(data2).asBytes());

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
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(MARKER, objectName + "/1")
                        .setQueryParam(END_MARKER, objectName + "/4"))
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
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case objectName + "/3":
                                    assertEquals(context, md52, hash);
                                    assertEquals(context, PLAIN_TEXT_UTF_8.toString(), mediaType);
                                    assertEquals(context, data2.length, bytes);
                                    break;
                                case objectName + "/2":
                                    assertEquals(context, md51, hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, data1.length, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testListDelimiterObjects(TestContext context) {
        final byte[] data0 = "HELLO0".getBytes(UTF_8);
        final byte[] data1 = "HELLO11".getBytes(UTF_8);
        final byte[] data2 = "HELLO222".getBytes(UTF_8);

        final String md50 = base16().lowerCase().encode(md5().hashBytes(data0).asBytes());
        final String md51 = base16().lowerCase().encode(md5().hashBytes(data1).asBytes());
        final String md52 = base16().lowerCase().encode(md5().hashBytes(data2).asBytes());

        Async async = context.async();
        prepareContainer(context)

                // put three objects then list and assert
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/animals/cats/persian.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/animals/cats/siamese.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/animals/dogs/corgi.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/animals/dogs/poodle.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/animals/dogs/terrier.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/me.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/plants/fern.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos/plants/rose.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutObject(httpClient, accountName, containerName, "photos2/plants/rose.jpg", authNonAdmin, new byte[]{}))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(httpClient, authAdmin))
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(DELIMITER, "/"))
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
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case "photos":
                                    assertEquals(context, "application/directory", mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                case "photos2":
                                    assertEquals(context, "application/directory", mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(PREFIX, "photos/")
                        .setQueryParam(DELIMITER, "/"))
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
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case "photos/animals":
                                    assertEquals(context, "application/directory", mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                case "photos/plants":
                                    assertEquals(context, "application/directory", mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                case "photos/me.jpg":
                                    assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(PREFIX, "photos/animals/dogs/")
                        .setQueryParam(DELIMITER, "/"))
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
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case "photos/animals/dogs/corgi.jpg":
                                    assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                case "photos/animals/dogs/poodle.jpg":
                                    assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                case "photos/animals/dogs/terrier.jpg":
                                    assertEquals(context, base16().lowerCase().encode(EMPTY_MD5), hash);
                                    assertEquals(context, OCTET_STREAM.toString(), mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .flatMap(new GetContainer(httpClient, accountName, containerName, authNonAdmin)
                        .setMediaTypes(JSON_UTF_8)
                        .setQueryParam(DELIMITER, "/")
                        .setQueryParam(MARKER, "photos"))
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
                            String hash = jsonObject.getString("hash");
                            String mediaType = jsonObject.getString("content_type");
                            long bytes = jsonObject.getLong("bytes");

                            switch (name) {
                                case "photos2":
                                    assertEquals(context, "application/directory", mediaType);
                                    assertEquals(context, 0, bytes);
                                    break;
                                default:
                                    context.fail();
                            }
                        }
                        return null;
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }


    @Test
    public void testEmptyContainer(TestContext context) {
        final String accountName = "testaccount";
        final String containerName = "testcontainer";
        ListMultimap<String, String> headers = create();


        Producer auth = httpBasic("admin", "admin");

        Async async = context.async();
        just((Void) null)
                .flatMap(new PostAccount(httpClient, accountName, auth))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_NO_CONTENT))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new PutContainer(httpClient, accountName, containerName, auth, headers))
                .map(new HttpClientResponseHeaderLogger())
                .map(new AssertHttpClientResponseStatusCode(context, HTTP_CREATED))
                .map(new ToVoid<HttpClientResponse>())
                .flatMap(new RefreshIndex(httpClient, authAdmin))
                .flatMap(new GetContainer(httpClient, accountName, containerName, auth)
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
                })
                .subscribe(new TestSubscriber(context, async));
    }
}
