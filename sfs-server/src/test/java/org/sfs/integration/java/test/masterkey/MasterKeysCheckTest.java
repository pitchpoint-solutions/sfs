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

package org.sfs.integration.java.test.masterkey;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.masterkey.UpdateMasterKey;
import org.sfs.encryption.MasterKeys;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.MasterKeysCheck;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.rx.BufferToJsonObject;
import org.sfs.rx.HttpClientKeepAliveResponseBodyBuffer;
import org.sfs.rx.ToType;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientResponseHeaderLogger;
import org.sfs.vo.PersistentMasterKey;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertFalse;
import static org.sfs.util.VertxAssert.assertTrue;
import static org.sfs.vo.PersistentMasterKey.fromSearchHit;
import static rx.Observable.just;

public class MasterKeysCheckTest extends BaseTestVerticle {

    private Producer authAdmin = httpBasic("admin", "admin");

    @Test
    public void testNoChanges(TestContext context) {
        byte[] data = "abc123".getBytes(UTF_8);
        AtomicReference<JsonObject> expectedJsonObject = new AtomicReference<>();
        MasterKeys masterKeys = VERTX_CONTEXT.verticle().masterKeys();
        Async async = context.async();
        just((Void) null)
                .flatMap(aVoid -> masterKeys.encrypt(VERTX_CONTEXT, data))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    SearchRequestBuilder request = elasticsearch.get()
                            .prepareSearch(elasticsearch.masterKeyTypeIndex())
                            .setTypes(elasticsearch.defaultType())
                            .setQuery(matchAllQuery())
                            .setVersion(true);
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultSearchTimeout());
                })
                .map(Optional::get)
                .map(searchResponse -> {
                    SearchHits searchHits = searchResponse.getHits();
                    SearchHit[] searchHitArray = searchHits.getHits();
                    assertEquals(context, 1, searchHitArray.length);
                    SearchHit searchHit = searchHitArray[0];
                    PersistentMasterKey persistentMasterKey = fromSearchHit(searchHit);
                    expectedJsonObject.set(persistentMasterKey.toJsonObject());
                    return (Void) null;
                })
                .flatMap(new MasterKeysCheck(HTTP_CLIENT, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(jsonObject -> {
                    assertEquals(context, HTTP_OK, jsonObject.getInteger("code").intValue());
                    assertEquals(context, "Success", jsonObject.getString("message"));
                    return jsonObject;
                })
                .map(new ToType<>((Void) null))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    SearchRequestBuilder request = elasticsearch.get()
                            .prepareSearch(elasticsearch.masterKeyTypeIndex())
                            .setTypes(elasticsearch.defaultType())
                            .setQuery(matchAllQuery())
                            .setVersion(true);
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultSearchTimeout());
                })
                .map(Optional::get)
                .map(searchResponse -> {
                    SearchHits searchHits = searchResponse.getHits();
                    SearchHit[] searchHitArray = searchHits.getHits();
                    assertEquals(context, 1, searchHitArray.length);
                    SearchHit searchHit = searchHitArray[0];
                    PersistentMasterKey persistentMasterKey = fromSearchHit(searchHit);
                    assertEquals(context, expectedJsonObject.get(), persistentMasterKey.toJsonObject());
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testHasPrimaryNoBackupNoHash(TestContext context) {
        byte[] data = "abc123".getBytes(UTF_8);
        MasterKeys masterKeys = VERTX_CONTEXT.verticle().masterKeys();
        Async async = context.async();
        just((Void) null)
                .flatMap(aVoid -> masterKeys.encrypt(VERTX_CONTEXT, data))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    SearchRequestBuilder request = elasticsearch.get()
                            .prepareSearch(elasticsearch.masterKeyTypeIndex())
                            .setTypes(elasticsearch.defaultType())
                            .setQuery(matchAllQuery())
                            .setVersion(true);
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultSearchTimeout());
                })
                .map(Optional::get)
                .map(searchResponse -> {
                    SearchHits searchHits = searchResponse.getHits();
                    SearchHit[] searchHitArray = searchHits.getHits();
                    assertEquals(context, 1, searchHitArray.length);
                    SearchHit searchHit = searchHitArray[0];
                    PersistentMasterKey persistentMasterKey = fromSearchHit(searchHit);
                    return persistentMasterKey.setBackup0KeyId(null)
                            .setBackup0EncryptedKey(null)
                            .setSecretSha512(null)
                            .setSecretSalt(null);
                })
                .flatMap(new UpdateMasterKey(VERTX_CONTEXT))
                .map(Optional::get)
                .map(persistentMasterKey -> {
                    assertFalse(context, persistentMasterKey.getBackup0KeyId().isPresent());
                    assertFalse(context, persistentMasterKey.getBackup0EncryptedKey().isPresent());
                    assertFalse(context, persistentMasterKey.getSecretSalt().isPresent());
                    assertFalse(context, persistentMasterKey.getSecretSha512().isPresent());
                    return (Void) null;
                })
                .map(new ToType<>((Void) null))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new MasterKeysCheck(HTTP_CLIENT, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(jsonObject -> {
                    assertEquals(context, HTTP_OK, jsonObject.getInteger("code").intValue());
                    assertEquals(context, "Success", jsonObject.getString("message"));
                    return jsonObject;
                })
                .map(new ToType<>((Void) null))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    SearchRequestBuilder request = elasticsearch.get()
                            .prepareSearch(elasticsearch.masterKeyTypeIndex())
                            .setTypes(elasticsearch.defaultType())
                            .setQuery(matchAllQuery())
                            .setVersion(true);
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultSearchTimeout());
                })
                .map(Optional::get)
                .map(searchResponse -> {
                    SearchHits searchHits = searchResponse.getHits();
                    SearchHit[] searchHitArray = searchHits.getHits();
                    assertEquals(context, 1, searchHitArray.length);
                    SearchHit searchHit = searchHitArray[0];
                    PersistentMasterKey persistentMasterKey = fromSearchHit(searchHit);
                    assertTrue(context, persistentMasterKey.getBackup0KeyId().isPresent());
                    assertTrue(context, persistentMasterKey.getBackup0EncryptedKey().isPresent());
                    assertTrue(context, persistentMasterKey.getSecretSalt().isPresent());
                    assertTrue(context, persistentMasterKey.getSecretSha512().isPresent());
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testHasPrimaryNoBackupHasHash(TestContext context) {
        byte[] data = "abc123".getBytes(UTF_8);
        MasterKeys masterKeys = VERTX_CONTEXT.verticle().masterKeys();
        Async async = context.async();
        just((Void) null)
                .flatMap(aVoid -> masterKeys.encrypt(VERTX_CONTEXT, data))
                .map(new ToVoid<>())
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    SearchRequestBuilder request = elasticsearch.get()
                            .prepareSearch(elasticsearch.masterKeyTypeIndex())
                            .setTypes(elasticsearch.defaultType())
                            .setQuery(matchAllQuery())
                            .setVersion(true);
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultSearchTimeout());
                })
                .map(Optional::get)
                .map(searchResponse -> {
                    SearchHits searchHits = searchResponse.getHits();
                    SearchHit[] searchHitArray = searchHits.getHits();
                    assertEquals(context, 1, searchHitArray.length);
                    SearchHit searchHit = searchHitArray[0];
                    PersistentMasterKey persistentMasterKey = fromSearchHit(searchHit);
                    return persistentMasterKey.setBackup0KeyId(null)
                            .setBackup0EncryptedKey(null);
                })
                .flatMap(new UpdateMasterKey(VERTX_CONTEXT))
                .map(Optional::get)
                .map(persistentMasterKey -> {
                    assertFalse(context, persistentMasterKey.getBackup0KeyId().isPresent());
                    assertFalse(context, persistentMasterKey.getBackup0EncryptedKey().isPresent());
                    assertTrue(context, persistentMasterKey.getSecretSalt().isPresent());
                    assertTrue(context, persistentMasterKey.getSecretSha512().isPresent());
                    return (Void) null;
                })
                .map(new ToType<>((Void) null))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(new MasterKeysCheck(HTTP_CLIENT, authAdmin))
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                .map(new HttpBodyLogger())
                .map(new BufferToJsonObject())
                .map(jsonObject -> {
                    assertEquals(context, HTTP_OK, jsonObject.getInteger("code").intValue());
                    assertEquals(context, "Success", jsonObject.getString("message"));
                    return jsonObject;
                })
                .map(new ToType<>((Void) null))
                .flatMap(new RefreshIndex(HTTP_CLIENT, authAdmin))
                .flatMap(aVoid -> {
                    Elasticsearch elasticsearch = VERTX_CONTEXT.verticle().elasticsearch();
                    SearchRequestBuilder request = elasticsearch.get()
                            .prepareSearch(elasticsearch.masterKeyTypeIndex())
                            .setTypes(elasticsearch.defaultType())
                            .setQuery(matchAllQuery())
                            .setVersion(true);
                    return elasticsearch.execute(VERTX_CONTEXT, request, elasticsearch.getDefaultSearchTimeout());
                })
                .map(Optional::get)
                .map(searchResponse -> {
                    SearchHits searchHits = searchResponse.getHits();
                    SearchHit[] searchHitArray = searchHits.getHits();
                    assertEquals(context, 1, searchHitArray.length);
                    SearchHit searchHit = searchHitArray[0];
                    PersistentMasterKey persistentMasterKey = fromSearchHit(searchHit);
                    assertTrue(context, persistentMasterKey.getBackup0KeyId().isPresent());
                    assertTrue(context, persistentMasterKey.getBackup0EncryptedKey().isPresent());
                    assertTrue(context, persistentMasterKey.getSecretSalt().isPresent());
                    assertTrue(context, persistentMasterKey.getSecretSha512().isPresent());
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

}