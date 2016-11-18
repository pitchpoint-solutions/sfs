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

import io.vertx.core.Vertx;
import org.sfs.SfsVertx;
import org.sfs.encryption.impl.SAES256v01;
import org.sfs.rx.RxHelper;
import org.sfs.util.PrngRandom;
import rx.Observable;

public enum AlgorithmDef {

    SALTED_AES256_V01("SAES256v01") {
        @Override
        public Algorithm create(byte[] secret, byte[] salt) {
            return new SAES256v01(secret, salt);
        }

        @Override
        public byte[] generateKeyBlocking() {
            byte[] key = new byte[SAES256v01.KEY_SIZE_BYTES];
            PrngRandom.getCurrentInstance().nextBytesBlocking(key);
            return key;
        }

        public byte[] generateSaltBlocking() {
            byte[] key = new byte[SAES256v01.NONCE_SIZE_BYTES];
            PrngRandom.getCurrentInstance().nextBytesBlocking(key);
            return key;
        }
    };

    private final String algorithmName;

    AlgorithmDef(String algorithmName) {
        this.algorithmName = algorithmName;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public static AlgorithmDef getPreferred() {
        return SALTED_AES256_V01;
    }

    /**
     * The algorithm instances via this method should accept a secret of any size and a salt of any size.
     * <p>
     * In cases where the secret is not equal to the key size the implementation of this method or the
     * implementation of the corresponding algorithm should adjust the secret size to the required size (ie.. sha526).
     * <p>
     * The same thing should be done with the salt
     *
     * @param secret
     * @param salt
     * @return
     */
    public abstract Algorithm create(byte[] secret, byte[] salt);

    public abstract byte[] generateKeyBlocking();

    public abstract byte[] generateSaltBlocking();

    public Observable<byte[]> generateKey(SfsVertx vertx) {
        return RxHelper.executeBlocking(vertx.getOrCreateContext(), vertx.getBackgroundPool(), this::generateKeyBlocking);
    }

    public Observable<byte[]> generateSalt(SfsVertx vertx) {
        return RxHelper.executeBlocking(vertx.getOrCreateContext(), vertx.getBackgroundPool(),this::generateSaltBlocking);
    }

    public static AlgorithmDef fromNameIfExists(String name) {
        for (AlgorithmDef algorithmDef : values()) {
            if (algorithmDef.algorithmName.equalsIgnoreCase(name)) {
                return algorithmDef;
            }
        }
        return null;
    }

}
