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

package org.sfs.filesystem;

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
import org.sfs.SfsVertx;
import org.sfs.SfsVertxImpl;
import org.sfs.TestSubscriber;
import org.sfs.io.Block;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.thread.NamedThreadFactory;
import rx.Observable;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.Files.hash;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.Boolean.TRUE;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.size;
import static java.nio.file.Files.write;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.sfs.filesystem.JournalFile.DEFAULT_BLOCK_SIZE;
import static org.sfs.io.Block.encodeFrame;
import static org.sfs.protobuf.XVolume.XJournal.Header.newBuilder;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.util.MessageDigestFactory.SHA512;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class JournalFileTest {

    private Path path;
    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();
    private SfsVertx sfsVertx;

    @Before
    public void start() {
        BlockingQueue<Runnable> ioQueue = new LinkedBlockingQueue<>(500);
        BlockingQueue<Runnable> backgroundQueue = new LinkedBlockingQueue<>(500);
        ExecutorService ioPool =
                new ThreadPoolExecutor(
                        200,
                        200,
                        0L,
                        MILLISECONDS,
                        ioQueue,
                        new NamedThreadFactory("sfs-io-pool"));
        ExecutorService backgroundPool =
                new ThreadPoolExecutor(
                        200,
                        200,
                        0L,
                        MILLISECONDS,
                        backgroundQueue,
                        new NamedThreadFactory("sfs-blocking-action-pool"));
        sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);
        try {
            path = createTempDirectory("");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void stop(TestContext context) {
        if (path != null) {
            rule.vertx().fileSystem().deleteRecursiveBlocking(path.toString(), true);
        }
    }

    @Test
    public void testHeaderFrameSize(TestContext context) {
        byte[] data =
                newBuilder()
                        .setNextHeaderPosition(MAX_VALUE)
                        .setMetaDataPosition(MAX_VALUE)
                        .setMetaDataLength(MAX_VALUE)
                        .setDataPosition(MAX_VALUE)
                        .setDataLength(MAX_VALUE)
                        .build()
                        .toByteArray();

        Buffer headerBuffer = buffer(data);
        Block.Frame<Buffer> headerFrame = encodeFrame(headerBuffer);
        Buffer headerFrameBuffer = headerFrame.getData();

        assertTrue(context, format("Header frame size was %d, expected at most %d", headerFrameBuffer.length(), DEFAULT_BLOCK_SIZE), headerFrameBuffer.length() <= DEFAULT_BLOCK_SIZE);

        data =
                newBuilder()
                        .setNextHeaderPosition(MIN_VALUE)
                        .setMetaDataPosition(MIN_VALUE)
                        .setMetaDataLength(MIN_VALUE)
                        .setDataPosition(MIN_VALUE)
                        .setDataLength(MIN_VALUE)
                        .build()
                        .toByteArray();

        headerBuffer = buffer(data);
        headerFrame = encodeFrame(headerBuffer);
        headerFrameBuffer = headerFrame.getData();


        assertTrue(context, format("Header frame size was %d, expected at most %d", headerFrameBuffer.length(), DEFAULT_BLOCK_SIZE), headerFrameBuffer.length() <= DEFAULT_BLOCK_SIZE);
    }

    @Test
    public void testMetadataAndNoBuffer(TestContext context) {
        Path journalPath = path.resolve(".journal");
        JournalFile journalFile = new JournalFile(journalPath);

        Async async = context.async();

        aVoid()
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> journalFile.enableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("hello0", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("hello1", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("hello2", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("hello3", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("hello4", UTF_8.toString())))
                // assert stuff before closing
                .flatMap(aVoid -> assertScanDataBuffer(context, sfsVertx, journalFile, 5, "hello", null))
                .flatMap(aVoid -> journalFile.disableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.force(sfsVertx, true))
                .flatMap(aVoid -> journalFile.close(sfsVertx))
                // assert stuff can be read closing and opening
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> assertScanDataBuffer(context, sfsVertx, journalFile, 5, "hello", null))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testMetadataAndBuffer(TestContext context) {
        Path journalPath = path.resolve(".journal");
        JournalFile journalFile = new JournalFile(journalPath);

        Async async = context.async();

        aVoid()
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> journalFile.enableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata0", UTF_8.toString()), buffer("data0", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata1", UTF_8.toString()), buffer("data1", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata2", UTF_8.toString()), buffer("data2", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata3", UTF_8.toString()), buffer("data3", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata4", UTF_8.toString()), buffer("data4", UTF_8.toString())))
                // assert stuff before closing
                .flatMap(aVoid -> assertScanDataBuffer(context, sfsVertx, journalFile, 5, "metadata", "data"))
                .flatMap(aVoid -> journalFile.disableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.force(sfsVertx, true))
                .flatMap(aVoid -> journalFile.close(sfsVertx))
                // assert stuff can be read closing and opening
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> assertScanDataBuffer(context, sfsVertx, journalFile, 5, "metadata", "data"))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testNoMetadataAndBuffer(TestContext context) {
        Path journalPath = path.resolve(".journal");
        JournalFile journalFile = new JournalFile(journalPath);

        Async async = context.async();

        aVoid()
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> journalFile.enableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer(0), buffer("data0", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer(0), buffer("data1", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer(0), buffer("data2", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer(0), buffer("data3", UTF_8.toString())))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer(0), buffer("data4", UTF_8.toString())))
                // assert stuff before closing
                .flatMap(aVoid -> assertScanDataBuffer(context, sfsVertx, journalFile, 5, null, "data"))
                .flatMap(aVoid -> journalFile.disableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.force(sfsVertx, true))
                .flatMap(aVoid -> journalFile.close(sfsVertx))
                // assert stuff can be read closing and opening
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> assertScanDataBuffer(context, sfsVertx, journalFile, 5, null, "data"))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testMetadataAndReadStream(TestContext context) throws IOException {

        byte[] data = new byte[256 * 1024 * 1024];
        getCurrentInstance().nextBytesBlocking(data);

        Path dataFile = path.resolve(".data");
        write(dataFile, data, CREATE_NEW);

        long size = size(dataFile);
        final byte[] expectedDataSha512 = hash(dataFile.toFile(), sha512()).asBytes();
        final AsyncFile bigFile = sfsVertx.fileSystem().openBlocking(dataFile.toString(), new OpenOptions());

        Path journalPath = path.resolve(".journal");
        JournalFile journalFile = new JournalFile(journalPath);

        Async async = context.async();

        aVoid()
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> journalFile.enableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata0", UTF_8.toString()), size, bigFile))
                .doOnNext(aVoid -> bigFile.setReadPos(0))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata1", UTF_8.toString()), size, bigFile))
                .doOnNext(aVoid -> bigFile.setReadPos(0))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata2", UTF_8.toString()), size, bigFile))
                .doOnNext(aVoid -> bigFile.setReadPos(0))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata3", UTF_8.toString()), size, bigFile))
                .doOnNext(aVoid -> bigFile.setReadPos(0))
                .flatMap(aVoid -> journalFile.append(sfsVertx, buffer("metadata4", UTF_8.toString()), size, bigFile))
                // assert stuff before closing
                .flatMap(aVoid -> assertScanDataReadStream(context, sfsVertx, journalFile, 5, "metadata", expectedDataSha512))
                .flatMap(aVoid -> journalFile.disableWrites(sfsVertx))
                .flatMap(aVoid -> journalFile.force(sfsVertx, true))
                .flatMap(aVoid -> journalFile.close(sfsVertx))
                // assert stuff can be read closing and opening
                .flatMap(aVoid -> journalFile.open(sfsVertx))
                .flatMap(aVoid -> assertScanDataReadStream(context, sfsVertx, journalFile, 5, "metadata", expectedDataSha512))
                .subscribe(new TestSubscriber(context, async));
    }

    private Observable<Void> assertScanDataBuffer(TestContext context, SfsVertx sfsVertx, JournalFile journalFile, int entryCount, String metadataPrefix, String dataPrefix) {
        AtomicInteger index = new AtomicInteger(0);
        return journalFile.scanFromFirst(sfsVertx, entry ->
                entry.getMetadata(sfsVertx)
                        .doOnNext(metadata -> {
                            if (metadataPrefix == null) {
                                assertEquals(context, 0, metadata.length());
                            } else {
                                assertEquals(context, metadataPrefix + index.get(), metadata.toString(UTF_8));
                            }
                        })
                        .flatMap(buffer -> {
                            BufferWriteEndableWriteStream bufferWriteStreamConsumer = new BufferWriteEndableWriteStream();
                            return entry.produceData(sfsVertx, bufferWriteStreamConsumer)
                                    .map(aVoid1 -> bufferWriteStreamConsumer.toBuffer())
                                    .doOnNext(data -> {
                                        if (dataPrefix == null) {
                                            assertEquals(context, 0, data.length());
                                        } else {
                                            assertEquals(context, dataPrefix + index.get(), data.toString(UTF_8));
                                        }
                                    });
                        })
                        .doOnNext(buffer -> index.getAndIncrement())
                        .map(buffer -> TRUE))
                .doOnNext(aVoid1 -> assertEquals(context, entryCount, index.get()));
    }

    private Observable<Void> assertScanDataReadStream(TestContext context, SfsVertx sfsVertx, JournalFile journalFile, int entryCount, String metadataPrefix, byte[] expectedDataSha512) {
        AtomicInteger index = new AtomicInteger(0);
        return journalFile.scanFromFirst(sfsVertx, entry ->
                entry.getMetadata(sfsVertx)
                        .doOnNext(metadata -> {
                            if (metadataPrefix == null) {
                                assertEquals(context, 0, metadata.length());
                            } else {
                                assertEquals(context, metadataPrefix + index.get(), metadata.toString(UTF_8));
                            }
                        })
                        .flatMap(buffer -> {
                            DigestEndableWriteStream digestWriteStreamConsumer = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512);
                            return entry.produceData(sfsVertx, digestWriteStreamConsumer)
                                    .map(aVoid1 -> digestWriteStreamConsumer.getDigest(SHA512).get())
                                    .doOnNext(actualDataSha512 -> {
                                        assertArrayEquals(context, expectedDataSha512, actualDataSha512);
                                    });
                        })
                        .doOnNext(buffer -> index.getAndIncrement())
                        .map(buffer -> TRUE))
                .doOnNext(aVoid1 -> assertEquals(context, entryCount, index.get()));
    }

}