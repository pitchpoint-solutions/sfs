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

import com.google.common.collect.Sets;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sfs.SfsVertx;
import org.sfs.SfsVertxImpl;
import org.sfs.TestSubscriber;
import org.sfs.io.AsyncFileReaderImpl;
import org.sfs.io.AsyncFileWriterImpl;
import org.sfs.io.AsyncIO;
import org.sfs.io.MultiEndableWriteStream;
import org.sfs.io.WriteQueueSupport;
import org.sfs.rx.ToVoid;
import rx.Observable;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(VertxUnitRunner.class)
public class MultiWriteStreamTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiWriteStreamTest.class);
    private ExecutorService backgroundBool = Executors.newFixedThreadPool(10);
    private ExecutorService ioPool = Executors.newFixedThreadPool(10);
    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    @After
    public void after(TestContext context) {
        backgroundBool.shutdown();
        ioPool.shutdown();
    }

    @Test
    public void test(TestContext context) throws IOException {
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundBool, ioPool);

        final byte[] dataBuffer = new byte[1024 * 1024 * 2];

        Arrays.fill(dataBuffer, (byte) 1);

        Path tmpFile = Files.createTempFile("", "");
        Files.write(tmpFile, dataBuffer, StandardOpenOption.WRITE, StandardOpenOption.SYNC);


        Async async = context.async();
        Observable.range(0, 5)
                .doOnNext(integer -> {
                    LOGGER.debug("Attempt " + integer);
                })
                .flatMap(integer -> {
                    try {

                        final Path out1 = Files.createTempFile("", "");
                        final Path out2 = Files.createTempFile("", "");
                        final Path out3 = Files.createTempFile("", "");

                        WriteQueueSupport q1 = new WriteQueueSupport(8192);
                        WriteQueueSupport q2 = new WriteQueueSupport(8192);
                        WriteQueueSupport q3 = new WriteQueueSupport(8192);
                        Set<OpenOption> options = Sets.newHashSet(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

                        AsynchronousFileChannel ain = AsynchronousFileChannel.open(tmpFile, options, backgroundBool);
                        AsynchronousFileChannel aout1 = AsynchronousFileChannel.open(out1, options, backgroundBool);
                        AsynchronousFileChannel aout2 = AsynchronousFileChannel.open(out2, options, backgroundBool);
                        AsynchronousFileChannel aout3 = AsynchronousFileChannel.open(out3, options, backgroundBool);
                        AsyncFileReaderImpl r1 = new AsyncFileReaderImpl(sfsVertx, 0, 8192, dataBuffer.length, ain, LOGGER);
                        AsyncFileWriterImpl w1 = new AsyncFileWriterImpl(0, q1, sfsVertx, aout1, LOGGER);
                        AsyncFileWriterImpl w2 = new AsyncFileWriterImpl(0, q2, sfsVertx, aout2, LOGGER);
                        AsyncFileWriterImpl w3 = new AsyncFileWriterImpl(0, q3, sfsVertx, aout3, LOGGER);
                        LOGGER.debug("Start Attempt " + integer + ", w1=" + w1 + ", w2=" + w2 + ", w3=" + w3);
                        MultiEndableWriteStream multiWriteStreamConsumer = new MultiEndableWriteStream(w1, w2, w3);
                        return AsyncIO.pump(r1, multiWriteStreamConsumer)
                                .doOnNext(aVoid1 -> {
                                    try {
                                        ain.close();
                                        aout1.close();
                                        aout2.close();
                                        aout3.close();
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    LOGGER.debug("Complete Attempt " + integer + ", w1=" + w1 + ", w2=" + w2 + ", w3=" + w3);
                                })
                                .doOnNext(aVoid1 -> {
                                    try {
                                        byte[] buffer1 = Files.readAllBytes(out1);
                                        byte[] buffer2 = Files.readAllBytes(out2);
                                        byte[] buffer3 = Files.readAllBytes(out3);

                                        VertxAssert.assertArrayEquals(context, dataBuffer, buffer1);
                                        VertxAssert.assertArrayEquals(context, dataBuffer, buffer2);
                                        VertxAssert.assertArrayEquals(context, dataBuffer, buffer3);

                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                });
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async) {
                    @Override
                    public void onNext(Void aVoid) {
                        request(1);
                    }

                    @Override
                    public void onStart() {
                        request(1);
                    }
                });

    }

}