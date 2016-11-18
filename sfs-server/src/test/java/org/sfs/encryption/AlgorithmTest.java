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

import com.google.common.io.ByteStreams;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.io.AsyncFileEndableWriteStream;
import org.sfs.io.AsyncIO;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.CipherEndableWriteStream;
import org.sfs.io.CipherReadStream;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.rx.Holder1;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.util.Buffers;
import org.sfs.util.MessageDigestFactory;
import org.sfs.util.PrngRandom;
import org.sfs.util.VertxAssert;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;

public class AlgorithmTest extends BaseTestVerticle {

    @Test
    public void testBuffered(TestContext context) {

        final byte[] salt = new byte[64];
        final byte[] secret = new byte[64];

        final byte[] data = new byte[1024];

        PrngRandom random = PrngRandom.getCurrentInstance();
        random.nextBytesBlocking(salt);
        random.nextBytesBlocking(secret);
        random.nextBytesBlocking(data);

        final CipherWriteStreamValidation cipherWriteStreamValidation = new CipherWriteStreamValidation(secret, salt);
        final byte[] expectedCipherBytes = cipherWriteStreamValidation.encrypt(data);
        final byte[] actualClearBytes = cipherWriteStreamValidation.decrypt(expectedCipherBytes);

        VertxAssert.assertArrayEquals(context, data, actualClearBytes);

        final Holder1<Integer> count = new Holder1<>();
        count.value = 0;

        Async async = context.async();
        Observable.from(AlgorithmDef.values())
                .map(new Func1<AlgorithmDef, Algorithm>() {
                    @Override
                    public Algorithm call(AlgorithmDef algorithmDef) {
                        count.value++;
                        return algorithmDef.create(secret, salt);
                    }
                })
                .map(new Func1<Algorithm, byte[]>() {
                    @Override
                    public byte[] call(final Algorithm algorithm) {
                        byte[] encrypted = algorithm.encrypt(data);
                        VertxAssert.assertArrayEquals(context, expectedCipherBytes, encrypted);
                        return algorithm.decrypt(encrypted);
                    }
                })
                .subscribe(new Subscriber<byte[]>() {
                    @Override
                    public void onCompleted() {
                        async.complete();
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                        context.fail(e);
                    }

                    @Override
                    public void onNext(byte[] bytes) {
                        VertxAssert.assertArrayEquals(context, data, bytes);
                        VertxAssert.assertEquals(context, AlgorithmDef.values().length, count.value.intValue());
                    }
                });
    }

    @Test
    public void testWriteStream(TestContext context) {

        final int bufferSize = 10;

        final byte[] salt = new byte[64];
        final byte[] secret = new byte[64];

        final byte[] data = new byte[bufferSize];

        PrngRandom random = PrngRandom.getCurrentInstance();
        random.nextBytesBlocking(salt);
        random.nextBytesBlocking(secret);
        random.nextBytesBlocking(data);

        CipherWriteStreamValidation cipherWriteStreamValidation = new CipherWriteStreamValidation(secret, salt);
        final byte[] expectedCipherBytes = cipherWriteStreamValidation.encrypt(data);
        final byte[] actualClearBytes = cipherWriteStreamValidation.decrypt(expectedCipherBytes);

        VertxAssert.assertArrayEquals(context, data, actualClearBytes);

        final Holder1<Integer> count = new Holder1<>();
        count.value = 0;

        Async async = context.async();
        Observable.from(AlgorithmDef.values())
                .map(new Func1<AlgorithmDef, Algorithm>() {
                    @Override
                    public Algorithm call(AlgorithmDef algorithmDef) {
                        count.value++;
                        return algorithmDef.create(secret, salt);
                    }
                })
                .flatMap(new Func1<Algorithm, Observable<byte[]>>() {
                    @Override
                    public Observable<byte[]> call(final Algorithm algorithm) {
                        final BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                        final BufferEndableWriteStream encryptedWriteStream = algorithm.encrypt(bufferWriteStream);
                        ObservableFuture<Void> handler = RxHelper.observableFuture();
                        encryptedWriteStream.endHandler(handler::complete);
                        for (Buffer partition : Buffers.partition(Buffer.buffer(data), bufferSize)) {
                            encryptedWriteStream.write(partition);
                        }
                        encryptedWriteStream.end();

                        return handler
                                .map(new Func1<Void, byte[]>() {
                                    @Override
                                    public byte[] call(Void aVoid) {
                                        return bufferWriteStream.toBuffer().getBytes();
                                    }
                                }).map(new Func1<byte[], byte[]>() {
                                    @Override
                                    public byte[] call(byte[] encrypted) {
                                        VertxAssert.assertArrayEquals(context, expectedCipherBytes, encrypted);
                                        return encrypted;
                                    }
                                })
                                .flatMap(new Func1<byte[], Observable<byte[]>>() {
                                    @Override
                                    public Observable<byte[]> call(byte[] encrypted) {
                                        final BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                                        final BufferEndableWriteStream decryptedWriteStream = algorithm.decrypt(bufferWriteStream);
                                        ObservableFuture<Void> handler = RxHelper.observableFuture();
                                        decryptedWriteStream.endHandler(handler::complete);
                                        for (Buffer partition : Buffers.partition(Buffer.buffer(encrypted), bufferSize)) {
                                            decryptedWriteStream.write(partition);
                                        }
                                        decryptedWriteStream.end();
                                        return handler
                                                .map(new Func1<Void, byte[]>() {
                                                    @Override
                                                    public byte[] call(Void aVoid) {
                                                        return bufferWriteStream.toBuffer().getBytes();
                                                    }
                                                })
                                                .map(new Func1<byte[], byte[]>() {
                                                    @Override
                                                    public byte[] call(byte[] decrypted) {
                                                        VertxAssert.assertArrayEquals(context, data, decrypted);
                                                        return decrypted;
                                                    }
                                                });
                                    }
                                });
                    }
                })
                .subscribe(new Subscriber<byte[]>() {
                    @Override
                    public void onCompleted() {
                        async.complete();
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                        context.fail(e);
                    }

                    @Override
                    public void onNext(byte[] bytes) {
                        VertxAssert.assertEquals(context, AlgorithmDef.values().length, count.value.intValue());
                    }
                });
    }

    @Test
    public void testFileStream(TestContext context) throws IOException {
        final byte[] salt = new byte[64];
        final byte[] secret = new byte[64];
        final byte[] dataBuffer = new byte[64 * 1024 * 1024];

        PrngRandom random = PrngRandom.getCurrentInstance();
        random.nextBytesBlocking(salt);
        random.nextBytesBlocking(secret);
        random.nextBytesBlocking(dataBuffer);

        CipherWriteStreamValidation cipherWriteStreamValidation = new CipherWriteStreamValidation(secret, salt);


        final Path encryptedTmpFile = Files.createTempFile(tmpDir, "", "");

        final byte[] expectedClearDigest;
        final byte[] expectedEncryptedDigest;

        DigestOutputStream encryptedDigestOutputStream = null;
        DigestOutputStream clearDigestOutputStream = null;
        try {
            encryptedDigestOutputStream = new DigestOutputStream(Files.newOutputStream(encryptedTmpFile), MessageDigestFactory.SHA512.instance());
            clearDigestOutputStream = new DigestOutputStream(cipherWriteStreamValidation.encrypt(encryptedDigestOutputStream), MessageDigestFactory.SHA512.instance());
            clearDigestOutputStream.write(dataBuffer);
        } finally {
            if (clearDigestOutputStream != null) {
                clearDigestOutputStream.close();
            }
        }

        expectedClearDigest = clearDigestOutputStream.getMessageDigest().digest();
        expectedEncryptedDigest = encryptedDigestOutputStream.getMessageDigest().digest();

        final byte[] actualClearDigest;
        byte[] actualEncryptedDigest;

        DigestInputStream encryptedDigestInputStream = null;
        DigestInputStream clearDigestInputStream = null;

        try {
            encryptedDigestInputStream = new DigestInputStream(Files.newInputStream(encryptedTmpFile), MessageDigestFactory.SHA512.instance());
            clearDigestInputStream = new DigestInputStream(cipherWriteStreamValidation.decrypt(encryptedDigestInputStream), MessageDigestFactory.SHA512.instance());
            OutputStream blackHole = ByteStreams.nullOutputStream();
            ByteStreams.copy(clearDigestInputStream, blackHole);
        } finally {
            if (clearDigestInputStream != null) {
                clearDigestInputStream.close();
            }
        }

        actualClearDigest = clearDigestInputStream.getMessageDigest().digest();
        actualEncryptedDigest = encryptedDigestInputStream.getMessageDigest().digest();

        VertxAssert.assertArrayEquals(context, expectedClearDigest, actualClearDigest);
        VertxAssert.assertArrayEquals(context, expectedEncryptedDigest, actualEncryptedDigest);


        final Path encryptedTmpFileVertx = Files.createTempFile(tmpDir, "", "");

        Async async = context.async();

        ObservableFuture<AsyncFile> rh = RxHelper.observableFuture();

        OpenOptions openOptions = new OpenOptions();
        openOptions.setCreate(true)
                .setRead(true)
                .setWrite(true);

        vertx.fileSystem()
                .open(encryptedTmpFileVertx.toString(), openOptions, rh.toHandler());

        rh
                .flatMap(new Func1<AsyncFile, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(AsyncFile asyncFile) {
                        BufferEndableWriteStream endable = new AsyncFileEndableWriteStream(asyncFile);
                        final DigestEndableWriteStream encryptedDigestWriteStream = new DigestEndableWriteStream(endable, MessageDigestFactory.SHA512);
                        Algorithm algorithm = AlgorithmDef.SALTED_AES256_V01.create(secret, salt);
                        CipherEndableWriteStream cipherWriteStream = algorithm.encrypt(encryptedDigestWriteStream);
                        final DigestEndableWriteStream clearDigestWriteStream = new DigestEndableWriteStream(cipherWriteStream, MessageDigestFactory.SHA512);
                        ObservableFuture<Void> handler = RxHelper.observableFuture();
                        clearDigestWriteStream.endHandler(handler::complete);
                        for (Buffer buffer : Buffers.partition(Buffer.buffer(dataBuffer), 8192)) {
                            clearDigestWriteStream.write(buffer);
                        }
                        clearDigestWriteStream.end();
                        return handler
                                .map(new Func1<Void, Void>() {
                                    @Override
                                    public Void call(Void aVoid) {
                                        VertxAssert.assertArrayEquals(context, expectedEncryptedDigest, encryptedDigestWriteStream.getDigest(MessageDigestFactory.SHA512).get());
                                        VertxAssert.assertArrayEquals(context, expectedClearDigest, clearDigestWriteStream.getDigest(MessageDigestFactory.SHA512).get());
                                        return null;
                                    }
                                });
                    }
                })
                .flatMap(new Func1<Void, Observable<AsyncFile>>() {
                    @Override
                    public Observable<AsyncFile> call(Void aVoid) {
                        ObservableFuture<AsyncFile> rh = RxHelper.observableFuture();

                        OpenOptions openOptions = new OpenOptions();
                        openOptions.setCreate(true)
                                .setRead(true)
                                .setWrite(true);

                        vertx.fileSystem()
                                .open(encryptedTmpFileVertx.toString(), openOptions, rh.toHandler());

                        return rh;
                    }
                })
                .flatMap(new Func1<AsyncFile, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(AsyncFile asyncFile) {
                        final DigestEndableWriteStream clearDigestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), MessageDigestFactory.SHA512);
                        Algorithm algorithm = AlgorithmDef.SALTED_AES256_V01.create(secret, salt);
                        CipherEndableWriteStream cipherWriteStream = algorithm.decrypt(clearDigestWriteStream);
                        final DigestEndableWriteStream encryptedDigestWriteStream = new DigestEndableWriteStream(cipherWriteStream, MessageDigestFactory.SHA512);
                        return AsyncIO.pump(asyncFile, encryptedDigestWriteStream)
                                .map(new Func1<Void, Void>() {
                                    @Override
                                    public Void call(Void aVoid) {
                                        VertxAssert.assertArrayEquals(context, expectedEncryptedDigest, encryptedDigestWriteStream.getDigest(MessageDigestFactory.SHA512).get());
                                        VertxAssert.assertArrayEquals(context, expectedClearDigest, clearDigestWriteStream.getDigest(MessageDigestFactory.SHA512).get());
                                        return null;
                                    }
                                });
                    }
                })
                .flatMap(new Func1<Void, Observable<AsyncFile>>() {
                    @Override
                    public Observable<AsyncFile> call(Void aVoid) {
                        ObservableFuture<AsyncFile> rh = RxHelper.observableFuture();

                        OpenOptions openOptions = new OpenOptions();
                        openOptions.setCreate(true)
                                .setRead(true)
                                .setWrite(true);

                        vertx.fileSystem()
                                .open(encryptedTmpFileVertx.toString(), openOptions, rh.toHandler());

                        return rh;
                    }
                })
                .flatMap(new Func1<AsyncFile, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(AsyncFile asyncFile) {
                        final DigestEndableWriteStream clearDigestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), MessageDigestFactory.SHA512);
                        Algorithm algorithm = AlgorithmDef.SALTED_AES256_V01.create(secret, salt);
                        CipherReadStream readStream = algorithm.decrypt(asyncFile);
                        return AsyncIO.pump(readStream, clearDigestWriteStream)
                                .map(new Func1<Void, Void>() {
                                    @Override
                                    public Void call(Void aVoid) {
                                        VertxAssert.assertArrayEquals(context, expectedClearDigest, clearDigestWriteStream.getDigest(MessageDigestFactory.SHA512).get());
                                        return null;
                                    }
                                });
                    }
                })
                .subscribe(new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        async.complete();
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                        context.fail(e);
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });


    }

}