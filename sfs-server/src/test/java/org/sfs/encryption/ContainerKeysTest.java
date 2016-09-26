/*
 *
 * Copyright (C) 2009 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.encryption;

import com.google.common.base.Optional;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.elasticsearch.account.LoadAccount;
import org.sfs.elasticsearch.container.LoadContainer;
import org.sfs.elasticsearch.containerkey.GetNewestContainerKey;
import org.sfs.elasticsearch.containerkey.UpdateContainerKey;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.AssertHttpClientResponseStatusCode;
import org.sfs.integration.java.func.PostAccount;
import org.sfs.integration.java.func.PostContainer;
import org.sfs.integration.java.func.PutContainer;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.rx.Holder2;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;

import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.String.valueOf;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.util.Calendar.getInstance;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.KnownMetadataKeys.X_MAX_OBJECT_REVISIONS;
import static org.sfs.util.SfsHttpHeaders.X_ADD_CONTAINER_META_PREFIX;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertFalse;
import static org.sfs.vo.ObjectPath.fromPaths;
import static rx.Observable.just;

public class ContainerKeysTest extends BaseTestVerticle {

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
    public void testKeyNoRotate(TestContext context) {
        ContainerKeys containerKeys = VERTX_CONTEXT.verticle().containerKeys();

        Async async = context.async();
        prepareContainer(context)
                .map(aVoid -> fromPaths(accountName).accountPath().get())
                .flatMap(new LoadAccount(VERTX_CONTEXT))
                .map(Optional::get)
                .flatMap(persistentAccount ->
                        just(fromPaths(accountName, containerName).containerPath().get())
                                .flatMap(new LoadContainer(VERTX_CONTEXT, persistentAccount))
                                .map(Optional::get))
                .flatMap(persistentContainer -> {
                    AtomicReference<byte[]> existingArray = new AtomicReference<>();
                    return containerKeys.preferredAlgorithm(VERTX_CONTEXT, persistentContainer)
                            .map(new ToVoid<>())
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .map(aVoid -> persistentContainer)
                            .flatMap(new GetNewestContainerKey(VERTX_CONTEXT))
                            .map(Optional::get)
                            .map(persistentContainerKey -> {
                                existingArray.set(persistentContainerKey.getEncryptedKey().get());
                                return persistentContainerKey;
                            })
                            .flatMap(pck -> containerKeys.rotateIfRequired(VERTX_CONTEXT, pck))
                            .map(persistentContainerKey -> {
                                assertEquals(context, "/testaccount/testcontainer/0000000000000000000", persistentContainerKey.getId());
                                assertArrayEquals(context, existingArray.get(), persistentContainerKey.getEncryptedKey().get());
                                return persistentContainer;
                            });
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testKeyRotate(TestContext context) {
        ContainerKeys containerKeys = VERTX_CONTEXT.verticle().containerKeys();

        Async async = context.async();
        prepareContainer(context)
                .map(aVoid -> fromPaths(accountName).accountPath().get())
                .flatMap(new LoadAccount(VERTX_CONTEXT))
                .map(Optional::get)
                .flatMap(persistentAccount ->
                        just(fromPaths(accountName, containerName).containerPath().get())
                                .flatMap(new LoadContainer(VERTX_CONTEXT, persistentAccount))
                                .map(Optional::get))
                .flatMap(persistentContainer -> {
                    AtomicReference<byte[]> existingArray = new AtomicReference<>();
                    AtomicReference<String> existingId = new AtomicReference<>();
                    return containerKeys.preferredAlgorithm(VERTX_CONTEXT, persistentContainer)
                            .map(new ToVoid<>())
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .map(aVoid -> persistentContainer)
                            .flatMap(new GetNewestContainerKey(VERTX_CONTEXT))
                            .map(Optional::get)
                            .map(persistentContainerKey -> {
                                assertEquals(context, "/testaccount/testcontainer/0000000000000000000", persistentContainerKey.getId());
                                existingArray.set(persistentContainerKey.getEncryptedKey().get());
                                existingId.set(persistentContainerKey.getId());
                                return persistentContainerKey;
                            })
                            .map(persistentContainerKey -> {
                                Calendar now = getInstance();
                                long thePast = DAYS.toMillis(365);
                                now.setTimeInMillis(thePast);
                                persistentContainerKey.setCreateTs(now);
                                return persistentContainerKey;
                            })
                            .flatMap(new UpdateContainerKey(VERTX_CONTEXT))
                            .map(Holder2::value1)
                            .map(Optional::get)
                            .flatMap(pck -> containerKeys.rotateIfRequired(VERTX_CONTEXT, pck))
                            .map(persistentContainerKey -> {
                                assertEquals(context, "/testaccount/testcontainer/0000000000000000001", persistentContainerKey.getId());
                                assertFalse(context, Arrays.equals(existingArray.get(), persistentContainerKey.getEncryptedKey().get()));
                                assertFalse(context, existingId.get().equals(persistentContainerKey.getId()));
                                return persistentContainer;
                            });
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testReEncrypt(TestContext context) {
        ContainerKeys containerKeys = VERTX_CONTEXT.verticle().containerKeys();

        Async async = context.async();
        prepareContainer(context)
                .map(aVoid -> fromPaths(accountName).accountPath().get())
                .flatMap(new LoadAccount(VERTX_CONTEXT))
                .map(Optional::get)
                .flatMap(persistentAccount ->
                        just(fromPaths(accountName, containerName).containerPath().get())
                                .flatMap(new LoadContainer(VERTX_CONTEXT, persistentAccount))
                                .map(Optional::get))
                .flatMap(persistentContainer -> {
                    AtomicReference<byte[]> existingArray = new AtomicReference<>();
                    AtomicReference<String> existingId = new AtomicReference<>();
                    return containerKeys.preferredAlgorithm(VERTX_CONTEXT, persistentContainer)
                            .map(new ToVoid<>())
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .map(aVoid -> persistentContainer)
                            .flatMap(new GetNewestContainerKey(VERTX_CONTEXT))
                            .map(Optional::get)
                            .map(persistentContainerKey -> {
                                assertEquals(context, "/testaccount/testcontainer/0000000000000000000", persistentContainerKey.getId());
                                existingArray.set(persistentContainerKey.getEncryptedKey().get());
                                existingId.set(persistentContainerKey.getId());
                                return persistentContainerKey;
                            })
                            .map(persistentContainerKey -> {
                                Calendar now = getInstance();
                                long thePast = DAYS.toMillis(365);
                                now.setTimeInMillis(thePast);
                                persistentContainerKey.setReEncryptTs(now);
                                return persistentContainerKey;
                            })
                            .flatMap(new UpdateContainerKey(VERTX_CONTEXT))
                            .map(new ToVoid<>())
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .flatMap(aVoid -> containerKeys.maintain(VERTX_CONTEXT))
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .map(aVoid -> persistentContainer)
                            .flatMap(new GetNewestContainerKey(VERTX_CONTEXT))
                            .map(Optional::get)
                            .map(persistentContainerKey -> {
                                assertEquals(context, "/testaccount/testcontainer/0000000000000000000", persistentContainerKey.getId());
                                assertFalse(context, Arrays.equals(existingArray.get(), persistentContainerKey.getEncryptedKey().get()));
                                return persistentContainer;
                            });
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testNoReEncrypt(TestContext context) {
        ContainerKeys containerKeys = VERTX_CONTEXT.verticle().containerKeys();

        Async async = context.async();
        prepareContainer(context)
                .map(aVoid -> fromPaths(accountName).accountPath().get())
                .flatMap(new LoadAccount(VERTX_CONTEXT))
                .map(Optional::get)
                .flatMap(persistentAccount ->
                        just(fromPaths(accountName, containerName).containerPath().get())
                                .flatMap(new LoadContainer(VERTX_CONTEXT, persistentAccount))
                                .map(Optional::get))
                .flatMap(persistentContainer -> {
                    AtomicReference<byte[]> existingArray = new AtomicReference<>();
                    AtomicReference<String> existingId = new AtomicReference<>();
                    return containerKeys.preferredAlgorithm(VERTX_CONTEXT, persistentContainer)
                            .map(new ToVoid<>())
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .map(aVoid -> persistentContainer)
                            .flatMap(new GetNewestContainerKey(VERTX_CONTEXT))
                            .map(Optional::get)
                            .map(persistentContainerKey -> {
                                assertEquals(context, "/testaccount/testcontainer/0000000000000000000", persistentContainerKey.getId());
                                existingArray.set(persistentContainerKey.getEncryptedKey().get());
                                existingId.set(persistentContainerKey.getId());
                                return persistentContainerKey;
                            })
                            .flatMap(new UpdateContainerKey(VERTX_CONTEXT))
                            .map(new ToVoid<>())
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .flatMap(aVoid -> containerKeys.maintain(VERTX_CONTEXT))
                            .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                            .map(aVoid -> persistentContainer)
                            .flatMap(new GetNewestContainerKey(VERTX_CONTEXT))
                            .map(Optional::get)
                            .map(persistentContainerKey -> {
                                assertEquals(context, "/testaccount/testcontainer/0000000000000000000", persistentContainerKey.getId());
                                assertArrayEquals(context, existingArray.get(), persistentContainerKey.getEncryptedKey().get());
                                return persistentContainer;
                            });
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testEncryptDecrypt(TestContext context) {

        byte[] data = "this is a test 111".getBytes(UTF_8);

        ContainerKeys containerKeys = VERTX_CONTEXT.verticle().containerKeys();

        Async async = context.async();
        prepareContainer(context)
                .map(aVoid -> fromPaths(accountName).accountPath().get())
                .flatMap(new LoadAccount(VERTX_CONTEXT))
                .map(Optional::get)
                .flatMap(persistentAccount ->
                        just(fromPaths(accountName, containerName).containerPath().get())
                                .flatMap(new LoadContainer(VERTX_CONTEXT, persistentAccount))
                                .map(Optional::get))
                .flatMap(persistentContainer ->
                        containerKeys.preferredAlgorithm(VERTX_CONTEXT, persistentContainer)
                                .flatMap(keyResponse -> {
                                    Algorithm algorithm = keyResponse.getData();
                                    byte[] encryptedData = algorithm.encrypt(data);
                                    return containerKeys.algorithm(VERTX_CONTEXT, persistentContainer, keyResponse.getKeyId(), keyResponse.getSalt())
                                            .map(keyResponse1 -> {
                                                Algorithm algorithm1 = keyResponse1.getData();
                                                return algorithm1.decrypt(encryptedData);
                                            });
                                }))
                .map(decryptedBytes -> {
                    assertArrayEquals(context, data, decryptedBytes);
                    return decryptedBytes;
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));
    }

}