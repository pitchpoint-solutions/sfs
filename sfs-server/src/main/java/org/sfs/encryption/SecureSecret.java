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

import com.google.common.primitives.Longs;
import org.sfs.rx.ResultMemoizeHandler;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

public final class SecureSecret {

    private byte[] salt;
    private byte[] encrypted;
    private final AlgorithmDef algorithmDef;

    public SecureSecret() {
        this.algorithmDef = AlgorithmDef.getPreferred();
        this.salt = algorithmDef.generateSalt();
    }

    public <T> Observable<T> apply(Func1<byte[], Observable<T>> function) {
        byte[] clear = getClearBytes();
        ResultMemoizeHandler<T> handler = new ResultMemoizeHandler<>();
        function.call(clear)
                .subscribe(new Subscriber<T>() {

                    T value;

                    @Override
                    public void onCompleted() {
                        Arrays.fill(clear, (byte) 0);
                        handler.complete(value);
                    }

                    @Override
                    public void onError(Throwable e) {
                        Arrays.fill(clear, (byte) 0);
                        handler.fail(e);
                    }

                    @Override
                    public void onNext(T t) {
                        value = t;
                    }
                });
        return Observable.create(handler.subscribe);

    }

    public byte[] getClearBytes() {
        if (encrypted == null) {
            throw new IllegalStateException("encrypted cannot be null");
        }
        if (salt == null) {
            throw new IllegalStateException("salt cannot be null");
        }
        Algorithm algorithm = algorithmDef.create(getSecret(), salt);
        return algorithm.decrypt(encrypted);
    }

    public SecureSecret setClearBytes(byte[] clear) {
        encrypted = encrypt(clear);
        return this;
    }

    protected byte[] encrypt(byte[] clear) {
        if (clear == null) {
            return null;
        }
        Algorithm algorithm = algorithmDef.create(getSecret(), salt);
        return algorithm.encrypt(clear);
    }

    protected byte[] toBytes(long v) {
        return Longs.toByteArray(v);
    }

    protected byte[] getSecret() {
        byte[] secret = toBytes(ManagementFactory.getRuntimeMXBean().getStartTime());
        return secret;
    }
}