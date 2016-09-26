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

package org.sfs.encryption.impl;

import com.google.common.hash.Hashing;
import com.google.common.math.LongMath;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.bouncycastle.crypto.engines.AESFastEngine;
import org.bouncycastle.crypto.io.CipherOutputStream;
import org.bouncycastle.crypto.modes.GCMBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.sfs.encryption.Algorithm;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.CipherEndableWriteStream;
import org.sfs.io.CipherReadStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class SAES256v01 extends Algorithm {

    public static final int KEY_SIZE_BYTES = 32;
    public static final int NONCE_SIZE_BYTES = 12;
    private static final int MAC_SIZE_BITS = 96;
    private byte[] salt;
    private final GCMBlockCipher encryptor;
    private final GCMBlockCipher decryptor;
    private static final long MAX_LONG_BUFFER_SIZE = Long.MAX_VALUE - 12;
    private static final int MAC_SIZE_BYTES = MAC_SIZE_BITS / 8;

    public SAES256v01(byte[] secretBytes, byte[] salt) {
        this.salt = salt.clone();
        secretBytes = secretBytes.clone();
        if (secretBytes.length != KEY_SIZE_BYTES) {
            secretBytes = Hashing.sha256().hashBytes(secretBytes).asBytes();
        }
        try {
            KeyParameter key = new KeyParameter(secretBytes);
            AEADParameters params = new AEADParameters(key, MAC_SIZE_BITS, this.salt);

            this.encryptor = new GCMBlockCipher(new AESFastEngine());
            this.encryptor.init(true, params);

            this.decryptor = new GCMBlockCipher(new AESFastEngine());
            this.decryptor.init(false, params);

        } catch (Exception e) {
            throw new RuntimeException("could not create cipher for AES256", e);
        } finally {
            Arrays.fill(secretBytes, (byte) 0);
        }
    }

    public long maxEncryptInputSize() {
        return MAX_LONG_BUFFER_SIZE;
    }

    @Override
    public long encryptOutputSize(long size) {
        try {
            return LongMath.checkedAdd(size, MAC_SIZE_BYTES);
        } catch (ArithmeticException e) {
            // do nothing
        }
        return -1;
    }

    @Override
    public byte[] getSalt() {
        return salt.clone();
    }


    @Override
    public byte[] encrypt(byte[] buffer) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (CipherOutputStream cipherOutputStream = new CipherOutputStream(byteArrayOutputStream, encryptor)) {
            cipherOutputStream.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public byte[] decrypt(byte[] buffer) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (CipherOutputStream cipherOutputStream = new CipherOutputStream(byteArrayOutputStream, decryptor)) {
            cipherOutputStream.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public Buffer encrypt(Buffer buffer) {
        return Buffer.buffer(encrypt(buffer.getBytes()));
    }

    @Override
    public Buffer decrypt(Buffer buffer) {
        return Buffer.buffer(decrypt(buffer.getBytes()));
    }

    @Override
    public <T extends CipherEndableWriteStream> T decrypt(BufferEndableWriteStream writeStream) {
        return (T) new CipherEndableWriteStream(writeStream, decryptor);
    }

    @Override
    public <T extends CipherEndableWriteStream> T encrypt(BufferEndableWriteStream writeStream) {
        return (T) new CipherEndableWriteStream(writeStream, encryptor);
    }

    @Override
    public <T extends CipherReadStream> T encrypt(ReadStream<Buffer> readStream) {
        return (T) new CipherReadStream(readStream, encryptor);
    }

    @Override
    public <T extends CipherReadStream> T decrypt(ReadStream<Buffer> readStream) {
        return (T) new CipherReadStream(readStream, decryptor);
    }
}