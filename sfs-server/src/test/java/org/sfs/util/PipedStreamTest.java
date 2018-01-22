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

package org.sfs.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sfs.RunTestOnContextRx;
import org.sfs.SfsVertx;
import org.sfs.SfsVertxImpl;
import org.sfs.TestSubscriber;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.io.PipedEndableWriteStream;
import org.sfs.io.PipedReadStream;
import org.sfs.thread.NamedCapacityFixedThreadPool;
import rx.Observable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.Files.hash;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.newOutputStream;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.util.MessageDigestFactory.SHA512;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.VertxAssert.assertArrayEquals;

@RunWith(VertxUnitRunner.class)
public class PipedStreamTest {

    @Rule
    public final RunTestOnContextRx rule = new RunTestOnContextRx();

    private ExecutorService backgroundPool;
    private ExecutorService ioPool;

    @Before
    public void start() {
        ioPool = NamedCapacityFixedThreadPool.newInstance(200, "sfs-io-pool");
        backgroundPool = NamedCapacityFixedThreadPool.newInstance(200, "sfs-blocking-action-pool");
    }

    @After
    public void stop(TestContext context) {
        if (ioPool != null) {
            ioPool.shutdown();
        }
        if (backgroundPool != null) {
            backgroundPool.shutdown();
        }
    }

    @Test
    public void testImmediateEnd(TestContext context) {

        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        Buffer testBuffer = buffer("test");
        Async async = context.async();
        aVoid()
                .flatMap(aVoid -> {
                    PipedReadStream pipedReadStream = new PipedReadStream();
                    PipedEndableWriteStream pipedEndableWriteStream = new PipedEndableWriteStream(pipedReadStream);
                    BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                    pipedEndableWriteStream.end(testBuffer);
                    return pump(pipedReadStream, bufferWriteStream)
                            .map(aVoid1 -> {
                                assertArrayEquals(context, testBuffer.getBytes(), bufferWriteStream.toBuffer().getBytes());
                                return (Void) null;
                            });
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testDelayedEnd(TestContext context) {
        SfsVertx vertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        Buffer testBuffer = buffer("test");
        Async async = context.async();
        aVoid()
                .flatMap(aVoid -> {
                    PipedReadStream pipedReadStream = new PipedReadStream();
                    PipedEndableWriteStream pipedEndableWriteStream = new PipedEndableWriteStream(pipedReadStream);
                    BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                    pipedEndableWriteStream.write(testBuffer);
                    return pump(pipedReadStream, bufferWriteStream)
                            .doOnSubscribe(() -> vertx.setTimer(500, event -> pipedEndableWriteStream.end()))
                            .map(aVoid1 -> {
                                assertArrayEquals(context, testBuffer.getBytes(), bufferWriteStream.toBuffer().getBytes());
                                return (Void) null;
                            });
                })
                .subscribe(new TestSubscriber(context, async));
    }


    @Test
    public void testImmediatePumpFile(TestContext context) throws IOException {
        SfsVertx vertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        byte[] bytes = new byte[256];
        getCurrentInstance().nextBytesBlocking(bytes);
        Path path = createTempFile("", "");

        try (OutputStream outputStream = newOutputStream(path)) {
            for (int i = 0; i < 10000; i++) {
                outputStream.write(bytes);
            }
        }

        final byte[] sha512 = hash(path.toFile(), sha512()).asBytes();

        Async async = context.async();
        aVoid()
                .flatMap(aVoid -> {
                    AsyncFile asyncFile = vertx.fileSystem().openBlocking(path.toString(), new OpenOptions());
                    PipedReadStream pipedReadStream = new PipedReadStream();
                    PipedEndableWriteStream pipedEndableWriteStream = new PipedEndableWriteStream(pipedReadStream);
                    Observable<Void> producer = pump(asyncFile, pipedEndableWriteStream);
                    DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512);
                    Observable<Void> consumer = pump(pipedReadStream, digestWriteStream);
                    return combineSinglesDelayError(producer, consumer, (aVoid1, aVoid2) -> {
                        assertArrayEquals(context, sha512, digestWriteStream.getDigest(SHA512).get());
                        return (Void) null;
                    });
                })
                .doOnTerminate(() -> {
                    try {
                        deleteIfExists(path);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }


}