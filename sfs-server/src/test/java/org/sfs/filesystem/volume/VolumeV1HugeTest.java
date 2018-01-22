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

package org.sfs.filesystem.volume;

import com.google.common.base.Optional;
import com.google.common.math.LongMath;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
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
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.CountingReadStream;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.DigestReadStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.io.ReplayReadStream;
import org.sfs.rx.ToVoid;
import org.sfs.thread.NamedCapacityFixedThreadPool;
import org.sfs.util.MessageDigestFactory;
import org.sfs.util.VertxAssert;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;


@RunWith(VertxUnitRunner.class)
public class VolumeV1HugeTest {

    private static Logger LOGGER = LoggerFactory.getLogger(VolumeV1HugeTest.class);
    private Path path;
    @Rule
    public final RunTestOnContextRx rule = new RunTestOnContextRx();
    private ExecutorService ioPool;
    private ExecutorService backgroundPool;

    @Before
    public void start() {
        try {
            path = Files.createTempDirectory("");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ioPool = NamedCapacityFixedThreadPool.newInstance(200, "sfs-io-pool");
        backgroundPool = NamedCapacityFixedThreadPool.newInstance(200, "sfs-blocking-action-pool");
    }

    @After
    public void stop(TestContext context) {
        if (path != null) {
            rule.vertx().fileSystem().deleteRecursiveBlocking(path.toString(), true);
        }
        if (ioPool != null) {
            ioPool.shutdown();
        }
        if (backgroundPool != null) {
            backgroundPool.shutdown();
        }
    }


    @Test(timeout = 300000)
    public void testRepeatReadStream(TestContext context) {
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);
        final VolumeV1 sfsDataV1 = new VolumeV1(path);
        final int repeat = 10;

        Async async = context.async();
        sequence(context, sfsVertx, sfsDataV1, repeat)
                .flatMap(aVoid -> sequence(context, sfsVertx, sfsDataV1, repeat))
                .repeat(10)
                .subscribe(new TestSubscriber(context, async));
    }

    @Test(timeout = 300000)
    public void testHugeReadStream(TestContext context) {
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);
        final VolumeV1 sfsDataV1 = new VolumeV1(path);
        final int repeat = 5000000;

        Async async = context.async();
        sequence(context, sfsVertx, sfsDataV1, repeat)
                .flatMap(aVoid -> sequence(context, sfsVertx, sfsDataV1, repeat)).repeat(5)
                .subscribe(new TestSubscriber(context, async));
    }

    protected Observable<Void> sequence(TestContext context, SfsVertx sfsVertx, final Volume volumeV1, final int howManyTimesToProviderTheBufferInTheReadStream) {
        final Calendar expectedCreateDateTime = Calendar.getInstance();
        expectedCreateDateTime.set(Calendar.YEAR, expectedCreateDateTime.get(Calendar.YEAR) + 1);

        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final long streamLength = LongMath.checkedMultiply(expectedBuffer.length(), howManyTimesToProviderTheBufferInTheReadStream);

        LOGGER.debug("Testing with " + streamLength + " byte(s) of data");
        final AtomicReference<byte[]> aDigest = new AtomicReference<>();


        return volumeV1.open(sfsVertx)
                .flatMap(aVoid -> {
                    final ReadStream<Buffer> replayReadStream = new ReplayReadStream(expectedBuffer, howManyTimesToProviderTheBufferInTheReadStream);
                    return volumeV1.putDataStream(sfsVertx, streamLength)
                            .flatMap(writeStreamBlob -> {
                                final DigestReadStream digestWriteStream = new DigestReadStream(replayReadStream, MessageDigestFactory.SHA512);
                                CountingReadStream countingWriteStream = new CountingReadStream(digestWriteStream);
                                return writeStreamBlob.consume(countingWriteStream)
                                        .map(aVoid1 -> {
                                            VertxAssert.assertEquals(context, streamLength, countingWriteStream.count());
                                            aDigest.set(digestWriteStream.getDigest(MessageDigestFactory.SHA512).get());
                                            return writeStreamBlob.getPosition();
                                        });
                            });
                }).flatMap(position ->
                        volumeV1.getDataStream(sfsVertx, position, Optional.absent(), Optional.absent())
                                .flatMap(readStreamBlobOptional -> {
                                    BufferEndableWriteStream nullWriteStream = new NullEndableWriteStream();
                                    final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(nullWriteStream, MessageDigestFactory.SHA512);
                                    return readStreamBlobOptional.get().produce(digestWriteStream)
                                            .map(aVoid -> {
                                                VertxAssert.assertArrayEquals(context, aDigest.get(), digestWriteStream.getDigest(MessageDigestFactory.SHA512).get());
                                                return readStreamBlobOptional.get().getPosition();
                                            });
                                })
                )
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, volumeV1));

    }

}

