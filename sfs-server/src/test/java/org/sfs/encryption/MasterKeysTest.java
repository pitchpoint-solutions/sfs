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

package org.sfs.encryption;

import com.google.common.base.Optional;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.elasticsearch.masterkey.GetNewestMasterKey;
import org.sfs.elasticsearch.masterkey.LoadMasterKey;
import org.sfs.elasticsearch.masterkey.UpdateMasterKey;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.RefreshIndex;
import org.sfs.rx.ToVoid;

import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Calendar.getInstance;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.sfs.encryption.MasterKeys.MasterKey;
import static org.sfs.integration.java.help.AuthorizationFactory.Producer;
import static org.sfs.integration.java.help.AuthorizationFactory.httpBasic;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertFalse;
import static rx.Observable.just;

public class MasterKeysTest extends BaseTestVerticle {

    private Producer authAdmin = httpBasic("admin", "admin");

    @Test
    public void testGetExistingGetsNew(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.getPreferredKey(vertxContext()))
                    .flatMap(masterKey -> {
                        String id = masterKey.getKeyId();
                        return masterKeys.getExistingKey(vertxContext(), id)
                                .map(Optional::get)
                                .map(masterKey1 -> {
                                    assertEquals(context, id, masterKey1.getKeyId());
                                    return (Void) null;
                                });
                    })
                    .map(new ToVoid<>());
        });
    }

    @Test
    public void testEncryptDecrypt(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            byte[] expectedDecrypted = "HELLO".getBytes(UTF_8);
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.encrypt(vertxContext(), expectedDecrypted))
                    .flatMap(encrypted ->
                            masterKeys.decrypt(vertxContext(), encrypted))
                    .map(Optional::get)
                    .map(decrypted -> {
                        assertArrayEquals(context, expectedDecrypted, decrypted);
                        return (Void) null;
                    });
        });

    }

    @Test
    public void testKeyRotate(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.getPreferredKey(vertxContext()))
                    .map(MasterKey::getKeyId)
                    .flatMap(new LoadMasterKey(vertxContext()))
                    .map(Optional::get)
                    .map(pmk -> {
                        assertEquals(context, masterKeys.firstKey(), pmk.getId());
                        Calendar now = getInstance();
                        long thePast = DAYS.toMillis(365);
                        now.setTimeInMillis(thePast);
                        pmk.setCreateTs(now);
                        return pmk;
                    })
                    .flatMap(pmk -> masterKeys.rotateIfRequired(vertxContext(), pmk, false))
                    .map(pmk -> {
                        assertEquals(context, masterKeys.nextKey(masterKeys.firstKey()), pmk.getId());
                        Calendar now = getInstance();
                        long thePast = DAYS.toMillis(365);
                        now.setTimeInMillis(thePast);
                        pmk.setCreateTs(now);
                        return pmk;
                    })
                    .flatMap(pmk -> masterKeys.rotateIfRequired(vertxContext(), pmk, false))
                    .map(pmk -> {
                        assertEquals(context, masterKeys.nextKey(masterKeys.nextKey(masterKeys.firstKey())), pmk.getId());
                        Calendar now = getInstance();
                        long thePast = DAYS.toMillis(365);
                        now.setTimeInMillis(thePast);
                        pmk.setCreateTs(now);
                        return pmk;
                    })
                    .map(new ToVoid<>());
        });
    }

    @Test
    public void testKeyReEncrypt(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            AtomicReference<byte[]> notExpectedArray = new AtomicReference<>();
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.getPreferredKey(vertxContext()))
                    .map(MasterKey::getKeyId)
                    .flatMap(new LoadMasterKey(vertxContext()))
                    .map(Optional::get)
                    .map(pmk -> {
                        assertEquals(context, masterKeys.firstKey(), pmk.getId());
                        Calendar now = getInstance();
                        long thePast = DAYS.toMillis(365);
                        now.setTimeInMillis(thePast);
                        pmk.setReEncrypteTs(now);
                        notExpectedArray.set(pmk.getEncryptedKey().get());
                        return pmk;
                    })
                    .flatMap(new UpdateMasterKey(vertxContext()))
                    .map(Optional::get)
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(aVoid -> masterKeys.maintain(vertxContext()))
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new GetNewestMasterKey(vertxContext(), AlgorithmDef.getPreferred()))
                    .map(Optional::get)
                    .map(pmk -> {
                        assertEquals(context, masterKeys.firstKey(), pmk.getId());
                        byte[] currentArray = pmk.getEncryptedKey().get();
                        assertFalse(context, Arrays.equals(notExpectedArray.get(), currentArray));
                        return pmk;
                    })
                    .map(new ToVoid<>());
        });
    }

    @Test
    public void testKeyNoReEncrypt(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            AtomicReference<byte[]> expectedArray = new AtomicReference<>();
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.getPreferredKey(vertxContext()))
                    .map(MasterKey::getKeyId)
                    .flatMap(new LoadMasterKey(vertxContext()))
                    .map(Optional::get)
                    .map(pmk -> {
                        assertEquals(context, masterKeys.firstKey(), pmk.getId());
                        Calendar now = getInstance();
                        pmk.setReEncrypteTs(now);
                        expectedArray.set(pmk.getEncryptedKey().get());
                        return pmk;
                    })
                    .flatMap(new UpdateMasterKey(vertxContext()))
                    .map(Optional::get)
                    .map(new ToVoid<>())
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(aVoid -> masterKeys.maintain(vertxContext()))
                    .flatMap(new RefreshIndex(httpClient(), authAdmin))
                    .flatMap(new GetNewestMasterKey(vertxContext(), AlgorithmDef.getPreferred()))
                    .map(Optional::get)
                    .map(pmk -> {
                        assertEquals(context, masterKeys.firstKey(), pmk.getId());
                        byte[] currentArray = pmk.getEncryptedKey().get();
                        assertArrayEquals(context, expectedArray.get(), currentArray);
                        return pmk;
                    })
                    .map(new ToVoid<>());
        });
    }

    @Test
    public void testExpireReEncryptTime(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.getPreferredKey(vertxContext()))
                    .map(pmk -> {
                        Calendar now = getInstance();
                        long thePast = DAYS.toMillis(365);
                        now.setTimeInMillis(thePast);
                        pmk.setReEncrypteTs(now);
                        return pmk;
                    })
                    .map(masterKey -> {
                        assertEquals(context, 1, masterKeys.cacheSize());
                        return masterKey;
                    })
                    .map(pmk -> {
                        masterKeys.expireCache();
                        return pmk;
                    })
                    .map(masterKey -> {
                        assertEquals(context, 0, masterKeys.cacheSize());
                        return masterKey;
                    })
                    .map(new ToVoid<>());
        });
    }

    @Test
    public void testExpireRotationTime(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.getPreferredKey(vertxContext()))
                    .map(pmk -> {
                        Calendar now = getInstance();
                        long thePast = DAYS.toMillis(365);
                        now.setTimeInMillis(thePast);
                        pmk.setCreateTs(now);
                        return pmk;
                    })
                    .map(masterKey -> {
                        assertEquals(context, 1, masterKeys.cacheSize());
                        return masterKey;
                    })
                    .map(pmk -> {
                        masterKeys.expireCache();
                        return pmk;
                    })
                    .map(masterKey -> {
                        assertEquals(context, 0, masterKeys.cacheSize());
                        return masterKey;
                    })
                    .map(new ToVoid<>());
        });
    }

    @Test
    public void testCacheIsBeingUsed(TestContext context) {
        runOnServerContext(context, () -> {
            MasterKeys masterKeys = vertxContext().verticle().masterKeys();
            return just((Void) null)
                    .flatMap(aVoid -> masterKeys.getPreferredKey(vertxContext()))
                    .flatMap(masterKey -> {
                        masterKeys.setFailIfNotCached(true);
                        return masterKeys.getExistingKey(vertxContext(), masterKey.getKeyId());
                    })
                    .map(new ToVoid<>());
        });
    }
}