/*
 * Copyright 2019 The Simple File Server Authors
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

package org.sfs.encryption.impl;

import com.google.common.hash.Hashing;
import com.google.common.math.LongMath;
import com.nimbusds.jose.crypto.BouncyCastleProviderSingleton;
import io.vertx.core.buffer.Buffer;
import org.sfs.encryption.Algorithm;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.CipherEndableWriteStream;
import org.sfs.io.CipherReadStream;
import org.sfs.io.EndableReadStream;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class SAES256v02 extends Algorithm {

    public static final int KEY_SIZE_BITS = 256;
    public static final int KEY_SIZE_BYTES = KEY_SIZE_BITS / 8;
    public static final int TAG_LENGTH_BITS = 96;
    public static final int NONCE_SIZE_BYTES = 12;
    private final byte[] salt;
    private final byte[] secret;
    private static final long MAX_LONG_BUFFER_SIZE = Long.MAX_VALUE - 12;
    private static final int TAG_LENGTH_BYTES = TAG_LENGTH_BITS / 8;

    public SAES256v02(byte[] secretBytes, byte[] salt) {
        this.salt = salt.clone();
        secretBytes = secretBytes.clone();
        if (secretBytes.length != KEY_SIZE_BYTES) {
            this.secret = Hashing.sha256().hashBytes(secretBytes).asBytes();
        } else {
            this.secret = secretBytes;
        }
    }

    public long maxEncryptInputSize() {
        return MAX_LONG_BUFFER_SIZE;
    }

    @Override
    public long encryptOutputSize(long size) {
        try {
            return LongMath.checkedAdd(size, TAG_LENGTH_BYTES);
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
        try {
            return forEncryption().doFinal(buffer);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] decrypt(byte[] buffer) {
        try {
            return forDecryption().doFinal(buffer);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
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
        return (T) new CipherEndableWriteStream(writeStream, forDecryption());
    }

    @Override
    public <T extends CipherEndableWriteStream> T encrypt(BufferEndableWriteStream writeStream) {
        return (T) new CipherEndableWriteStream(writeStream, forEncryption());
    }

    @Override
    public <T extends CipherReadStream> T encrypt(EndableReadStream<Buffer> readStream) {
        return (T) new CipherReadStream(readStream, forEncryption());
    }

    @Override
    public <T extends CipherReadStream> T decrypt(EndableReadStream<Buffer> readStream) {
        return (T) new CipherReadStream(readStream, forDecryption());
    }

    protected Cipher forEncryption() {
        try {
            SecretKeySpec key = new SecretKeySpec(secret, "AES");

            GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, this.salt);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", BouncyCastleProviderSingleton.getInstance());
            cipher.init(Cipher.ENCRYPT_MODE, key, spec);
            return cipher;
        } catch (Exception e) {
            throw new RuntimeException("could not create cipher for AES256", e);
        }
    }

    protected Cipher forDecryption() {
        try {
            SecretKeySpec key = new SecretKeySpec(secret, "AES");

            GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BITS, this.salt);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding", BouncyCastleProviderSingleton.getInstance());
            cipher.init(Cipher.DECRYPT_MODE, key, spec);
            return cipher;
        } catch (Exception e) {
            throw new RuntimeException("could not create cipher for AES256", e);
        }
    }
}