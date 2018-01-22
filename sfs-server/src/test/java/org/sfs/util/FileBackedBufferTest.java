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
import io.vertx.core.streams.ReadStream;
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
import org.sfs.io.BufferReadStream;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.FileBackedBuffer;
import org.sfs.thread.NamedCapacityFixedThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static java.nio.file.Files.createTempDirectory;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class FileBackedBufferTest {

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
    public void testSmallNoEncrypt(TestContext context) throws IOException {
        testSmall(context, true);
    }

    @Test
    public void testSmallEncrypt(TestContext context) throws IOException {
        testSmall(context, false);
    }

    public void testSmall(TestContext context, boolean encrypt) throws IOException {

        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);
        Path tmpDir = createTempDirectory(valueOf(currentTimeMillis()));

        Buffer testBuffer = buffer("test");
        Async async = context.async();
        aVoid()
                .flatMap(aVoid -> {
                    FileBackedBuffer fileBackedBuffer = new FileBackedBuffer(sfsVertx, 2, encrypt, tmpDir);
                    ReadStream<Buffer> readStream = new BufferReadStream(testBuffer);
                    return pump(readStream, fileBackedBuffer)
                            .flatMap(aVoid1 -> {
                                ReadStream<Buffer> fileBackedReadStream = fileBackedBuffer.readStream();
                                BufferWriteEndableWriteStream bufferedWriteStreamConsumer = new BufferWriteEndableWriteStream();
                                return pump(fileBackedReadStream, bufferedWriteStreamConsumer)
                                        .doOnNext(aVoid2 -> {
                                            Buffer actualBuffer = bufferedWriteStreamConsumer.toBuffer();
                                            assertArrayEquals(context, testBuffer.getBytes(), actualBuffer.getBytes());
                                            assertEquals(context, true, fileBackedBuffer.isFileOpen());
                                        });
                            })
                            .flatMap(aVoid1 -> fileBackedBuffer.close());
                })
                .subscribe(new TestSubscriber(context, async));
    }


    @Test
    public void testLargeNoEncrypt(TestContext context) throws IOException {
        testLarge(context, false);
    }


    @Test
    public void testLargeEncrypt(TestContext context) throws IOException {
        testLarge(context, true);
    }

    public void testLarge(TestContext context, boolean encrypt) throws IOException {

        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);
        Path tmpDir = createTempDirectory(valueOf(currentTimeMillis()));

        byte[] data = new byte[1024 * 1024 * 10];
        getCurrentInstance().nextBytesBlocking(data);
        Buffer testBuffer = buffer(data);
        Async async = context.async();
        aVoid()
                .flatMap(aVoid -> {
                    FileBackedBuffer fileBackedBuffer = new FileBackedBuffer(sfsVertx, 1024, encrypt, tmpDir);
                    ReadStream<Buffer> readStream = new BufferReadStream(testBuffer);
                    return pump(readStream, fileBackedBuffer)
                            .flatMap(aVoid1 -> {
                                ReadStream<Buffer> fileBackedReadStream = fileBackedBuffer.readStream();
                                BufferWriteEndableWriteStream bufferedWriteStreamConsumer = new BufferWriteEndableWriteStream();
                                return pump(fileBackedReadStream, bufferedWriteStreamConsumer)
                                        .doOnNext(aVoid2 -> {
                                            Buffer actualBuffer = bufferedWriteStreamConsumer.toBuffer();
                                            assertArrayEquals(context, testBuffer.getBytes(), actualBuffer.getBytes());
                                            assertEquals(context, true, fileBackedBuffer.isFileOpen());
                                        });
                            })
                            .flatMap(aVoid1 -> fileBackedBuffer.close());
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testOnlyBufferNoEncrypt(TestContext context) throws IOException {
        testOnlyBuffer(context, false);
    }

    @Test
    public void testOnlyBufferEncrypt(TestContext context) throws IOException {
        testOnlyBuffer(context, true);
    }


    public void testOnlyBuffer(TestContext context, boolean encrypt) throws IOException {

        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);
        Path tmpDir = createTempDirectory(valueOf(currentTimeMillis()));

        Buffer testBuffer = buffer("test");
        Async async = context.async();
        aVoid()
                .flatMap(aVoid -> {
                    FileBackedBuffer fileBackedBuffer = new FileBackedBuffer(sfsVertx, 1024, encrypt, tmpDir);
                    ReadStream<Buffer> readStream = new BufferReadStream(testBuffer);
                    return pump(readStream, fileBackedBuffer)
                            .flatMap(aVoid1 -> {
                                ReadStream<Buffer> fileBackedReadStream = fileBackedBuffer.readStream();
                                BufferWriteEndableWriteStream bufferedWriteStreamConsumer = new BufferWriteEndableWriteStream();
                                return pump(fileBackedReadStream, bufferedWriteStreamConsumer)
                                        .doOnNext(aVoid2 -> {
                                            Buffer actualBuffer = bufferedWriteStreamConsumer.toBuffer();
                                            assertArrayEquals(context, testBuffer.getBytes(), actualBuffer.getBytes());
                                            assertEquals(context, false, fileBackedBuffer.isFileOpen());
                                        });
                            })
                            .flatMap(aVoid1 -> fileBackedBuffer.close());
                })
                .subscribe(new TestSubscriber(context, async));
    }
}