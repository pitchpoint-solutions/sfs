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

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Assert;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.util.PrngRandom;
import rx.Observable;
import rx.functions.Func1;

import static java.lang.Long.MAX_VALUE;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.VertxAssert.assertEquals;
import static rx.Observable.from;
import static rx.Observable.range;

public class AlgorithmKeyResponseSizeCalculatorTest extends BaseTestVerticle {

    @Test
    public void testMaxEncryptInputSize(TestContext context) {
        final byte[] salt = new byte[64];
        final byte[] secret = new byte[64];

        PrngRandom random = getCurrentInstance();
        random.nextBytes(salt);
        random.nextBytes(secret);


        for (AlgorithmDef algorithmDef : AlgorithmDef.values()) {
            Algorithm algorithm = algorithmDef.create(secret, salt);
            for (long i = MAX_VALUE; i >= MAX_VALUE - 100; i--) {
                long encryptedSize = algorithm.encryptOutputSize(i);
                // loop downwards from max int found the first final
                // size that doesn't overflow
                if (encryptedSize > 0) {
                    assertEquals(context, i, algorithm.maxEncryptInputSize());
                    break;
                }
            }
        }
    }

    @Test
    public void testCalculateEncryptedSize(TestContext context) {
        final byte[] salt = new byte[64];
        final byte[] secret = new byte[64];

        PrngRandom random = getCurrentInstance();
        random.nextBytes(salt);
        random.nextBytes(secret);


        Async async = context.async();
        from(AlgorithmDef.values())
                .flatMap(new Func1<AlgorithmDef, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(AlgorithmDef algorithmDef) {
                        Algorithm algorithm = algorithmDef.create(secret, salt);
                        return range(0, 5 * 1024)
                                .map(new Func1<Integer, Void>() {
                                    @Override
                                    public Void call(Integer bufferSize) {
                                        byte[] buffer = new byte[bufferSize + 1];
                                        byte[] encryptedBuffer = algorithm.encrypt(buffer);
                                        long expectedEncryptedSize = algorithm.encryptOutputSize(buffer.length);
                                        Assert.assertEquals(expectedEncryptedSize, encryptedBuffer.length);
                                        return null;
                                    }
                                });
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }
}
