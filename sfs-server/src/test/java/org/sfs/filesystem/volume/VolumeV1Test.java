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
import io.vertx.core.buffer.Buffer;
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
import org.sfs.filesystem.ChecksummedPositional;
import org.sfs.filesystem.Positional;
import org.sfs.io.BufferReadStream;
import org.sfs.math.Rounding;
import org.sfs.protobuf.XVolume;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.thread.CapacityLinkedBlockingQueue;
import org.sfs.thread.NamedThreadFactory;
import org.sfs.util.VertxAssert;
import org.sfs.vo.TransientXVolume;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(VertxUnitRunner.class)
public class VolumeV1Test {

    private Path path;
    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void start() {
        try {
            path = Files.createTempDirectory("");
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
    public void testWriteMany(TestContext context) {
        BlockingQueue<Runnable> ioQueue = new CapacityLinkedBlockingQueue<>(500);
        BlockingQueue<Runnable> backgroundQueue = new CapacityLinkedBlockingQueue<>(500);

        ExecutorService ioPool =
                new ThreadPoolExecutor(
                        200,
                        200,
                        0L,
                        TimeUnit.MILLISECONDS,
                        ioQueue,
                        new NamedThreadFactory("sfs-io-pool"));
        ExecutorService backgroundPool =
                new ThreadPoolExecutor(
                        200,
                        200,
                        0L,
                        TimeUnit.MILLISECONDS,
                        backgroundQueue,
                        new NamedThreadFactory("sfs-blocking-action-pool"));
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);
        final VolumeV1 sfsDataV1 = new VolumeV1(path);
        AtomicInteger count = new AtomicInteger(0);
        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(aVoid -> {
                    ObservableFuture<Void> handler = RxHelper.observableFuture();
                    rule.vertx().setPeriodic(1000, event -> sfsDataV1.volumeInfo(sfsVertx)
                            .doOnNext(transientXVolume -> System.out.println("Stats: " + transientXVolume.toJsonObject().encodePrettily()))
                            .doOnNext(transientXVolume -> System.out.println("sfs-io-pool-size: " + ioQueue.size()))
                            .doOnNext(transientXVolume -> System.out.println("sfs-blocking-action-pool-size: " + backgroundQueue.size()))
                            .subscribe(new Subscriber<TransientXVolume>() {
                                @Override
                                public void onCompleted() {

                                }

                                @Override
                                public void onError(Throwable e) {

                                }

                                @Override
                                public void onNext(TransientXVolume transientXVolume) {

                                }
                            }));

                    Observable.interval(1, TimeUnit.MILLISECONDS, RxHelper.scheduler(rule.vertx().getOrCreateContext()))
                            .limit(10000)
                            .map(aLong -> count.getAndIncrement())
                            .doOnNext(integer -> {
                                if (integer % 100 == 0) {
                                    System.out.println("Writing " + integer);
                                }
                            })
                            .flatMap(integer -> {
                                Buffer buffer = Buffer.buffer("HELLO" + integer);
                                BufferReadStream bufferReadStream = new BufferReadStream(buffer);
                                return sfsDataV1.putDataStream(sfsVertx, buffer.length())
                                        .flatMap(writeStreamBlob -> writeStreamBlob.consume(bufferReadStream))
                                        .map(aVoid1 -> integer);
                            })
                            .doOnNext(integer -> {
                                if (integer % 100 == 0) {
                                    System.out.println("Wrote " + integer);
                                }
                            })
                            .subscribe(new Subscriber<Integer>() {
                                @Override
                                public void onStart() {
                                    request(1);
                                }

                                @Override
                                public void onCompleted() {
                                    handler.complete(null);
                                }

                                @Override
                                public void onError(Throwable e) {
                                    handler.fail(e);
                                }

                                @Override
                                public void onNext(Integer innteger) {
                                    request(1);

                                }
                            });
                    return handler;
                })
                .flatMap(aVoid -> sfsDataV1.close(sfsVertx))
                .flatMap(aVoid -> sfsDataV1.open(sfsVertx))
                .flatMap(aVoid -> sfsDataV1.volumeInfo(sfsVertx)
                        .doOnNext(transientXVolume -> System.out.println("Stats: " + transientXVolume.toJsonObject().encodePrettily()))
                        .map(transientXVolume -> (Void) null))
                .flatMap(aVoid -> sfsDataV1.close(sfsVertx))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testSingleWriteReadHashSizeAckDeleteTtlCreateDateTime(TestContext context) {

        ExecutorService backgroundPool = Executors.newFixedThreadPool(200);
        ExecutorService ioPool = Executors.newFixedThreadPool(200);
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final VolumeV1 sfsDataV1 = new VolumeV1(path);

        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(new PutFile<Void>(context, sfsVertx, sfsDataV1, expectedBuffer, 0L))
                .flatMap(new GetFile(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new FileHash(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new FileSize(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new AckFile(context, sfsVertx, sfsDataV1))
                .flatMap(new AssertHeader(
                        context,
                        sfsVertx,
                        sfsDataV1,
                        XVolume.XIndexBlock.newBuilder()
                                .setDeleted(false)
                                .setAcknowledged(true)
                                .setDataLength(expectedBuffer.length())
                                .setDataPosition(0L)
                                .setUpdatedTs(System.currentTimeMillis())
                                .build()))
                .flatMap(new DeleteFile(context, sfsVertx, sfsDataV1))
                .flatMap(new AssertHeader(
                        context,
                        sfsVertx,
                        sfsDataV1,
                        XVolume.XIndexBlock.newBuilder()
                                .setDeleted(true)
                                .setAcknowledged(true)
                                .setGarbageCollected(false)
                                .setDataLength(expectedBuffer.length())
                                .setDataPosition(0L)
                                .setUpdatedTs(System.currentTimeMillis())
                                .build()))
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, sfsDataV1))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testSingleDeleteRealloc(TestContext context) {

        ExecutorService backgroundPool = Executors.newFixedThreadPool(200);
        ExecutorService ioPool = Executors.newFixedThreadPool(200);
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final VolumeV1 sfsDataV1 = new VolumeV1(path);

        long inThePast = System.currentTimeMillis() - VolumeV1.MAX_GC_AGE - 1;

        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(inThePast);

        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(new PutFile<Void>(context, sfsVertx, sfsDataV1, expectedBuffer, 0L))
                .flatMap(new GetFile(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new DeleteFile(context, sfsVertx, sfsDataV1))
                .flatMap(new SetUpdateDateTime(sfsVertx, sfsDataV1, inThePast))
                .flatMap(new Reclaim(sfsVertx, sfsDataV1))
                .flatMap(new PutFile<Long>(context, sfsVertx, sfsDataV1, expectedBuffer, 0L))
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, sfsDataV1))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testSingleSweeperUnacknowledgedRealloc(TestContext context) {

        ExecutorService backgroundPool = Executors.newFixedThreadPool(200);
        ExecutorService ioPool = Executors.newFixedThreadPool(200);
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final VolumeV1 sfsDataV1 = new VolumeV1(path);

        final long expectedWritePosition = 0L;

        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(new PutFile<Void>(context, sfsVertx, sfsDataV1, expectedBuffer, expectedWritePosition))
                .flatMap(new GetFile(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new SetUpdateDateTime(sfsVertx, sfsDataV1, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(20)))
                .flatMap(new Reclaim(sfsVertx, sfsDataV1))
                .flatMap(new SetUpdateDateTime(sfsVertx, sfsDataV1, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(20)))
                .flatMap(new Reclaim(sfsVertx, sfsDataV1))
                .flatMap(new PutFile<Long>(context, sfsVertx, sfsDataV1, expectedBuffer, expectedWritePosition))
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, sfsDataV1))
                .subscribe(new TestSubscriber(context, async));
    }


    @Test
    public void testSingleSweeperDeleteRealloc(TestContext context) {

        ExecutorService backgroundPool = Executors.newFixedThreadPool(200);
        ExecutorService ioPool = Executors.newFixedThreadPool(200);
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        final Calendar oneYearAgo = Calendar.getInstance();
        oneYearAgo.setTimeInMillis(System.currentTimeMillis() - (VolumeV1.MAX_GC_AGE * 2));


        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final VolumeV1 sfsDataV1 = new VolumeV1(path);

        final long expectedWritePosition = 0L;

        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(new PutFile<Void>(context, sfsVertx, sfsDataV1, expectedBuffer, expectedWritePosition))
                .flatMap(new GetFile(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new AckFile(context, sfsVertx, sfsDataV1))
                .flatMap(new SetUpdateDateTime(sfsVertx, sfsDataV1, oneYearAgo.getTimeInMillis()))
                .flatMap(new Func1<Long, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(final Long position) {
                        return sfsDataV1.getIndexBlock0(sfsVertx, position)
                                .flatMap(new Func1<Optional<ChecksummedPositional<XVolume.XIndexBlock>>, Observable<? extends Long>>() {
                                    @Override
                                    public Observable<? extends Long> call(Optional<ChecksummedPositional<XVolume.XIndexBlock>> optional) {
                                        Positional<XVolume.XIndexBlock> positional = optional.get();
                                        return sfsDataV1.setIndexBlock0(
                                                sfsVertx,
                                                positional.getPosition(),
                                                positional.getValue()
                                                        .toBuilder()
                                                        .setDeleted(true)
                                                        .build())
                                                .map(new Func1<Void, Long>() {
                                                    @Override
                                                    public Long call(Void aVoid) {
                                                        return position;
                                                    }
                                                });
                                    }
                                });
                    }
                })
                .flatMap(new Reclaim(sfsVertx, sfsDataV1))
                .flatMap(new PutFile<Long>(context, sfsVertx, sfsDataV1, expectedBuffer, expectedWritePosition))
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, sfsDataV1))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testGetDataStreamWithInvalidPosition(TestContext context) {

        ExecutorService backgroundPool = Executors.newFixedThreadPool(200);
        ExecutorService ioPool = Executors.newFixedThreadPool(200);
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        final Calendar oneYearAgo = Calendar.getInstance();
        oneYearAgo.set(Calendar.YEAR, oneYearAgo.get(Calendar.YEAR) - 1);


        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final VolumeV1 sfsDataV1 = new VolumeV1(path);

        final long expectedWritePosition = 0L;

        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(new PutFile<Void>(context, sfsVertx, sfsDataV1, expectedBuffer, expectedWritePosition))
                .map(new Func1<Long, Long>() {
                    @Override
                    public Long call(Long aLong) {
                        return Rounding.down(Long.MAX_VALUE / 2, VolumeV1.INDEX_BLOCK_SIZE);
                    }
                })
                .flatMap(new Func1<Long, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(final Long position) {
                        return sfsDataV1.getDataStream(sfsVertx, position, Optional.absent(), Optional.absent())
                                .flatMap(new Func1<Optional<ReadStreamBlob>, Observable<Long>>() {
                                    @Override
                                    public Observable<Long> call(Optional<ReadStreamBlob> readStreamBlobOptional) {
                                        VertxAssert.assertFalse(context, readStreamBlobOptional.isPresent());
                                        return Observable.just(position);
                                    }
                                });
                    }
                })
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, sfsDataV1))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testSetWithInvalidPosition(TestContext context) {
        ExecutorService backgroundPool = Executors.newFixedThreadPool(200);
        ExecutorService ioPool = Executors.newFixedThreadPool(200);
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        final Calendar oneYearAgo = Calendar.getInstance();
        oneYearAgo.set(Calendar.YEAR, oneYearAgo.get(Calendar.YEAR) - 1);


        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final VolumeV1 sfsDataV1 = new VolumeV1(path);

        final long expectedWritePosition = 0L;

        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(new PutFile<Void>(context, sfsVertx, sfsDataV1, expectedBuffer, expectedWritePosition))
                .map(new Func1<Long, Long>() {
                    @Override
                    public Long call(Long position) {
                        return Rounding.down(Long.MAX_VALUE - 1000000, VolumeV1.INDEX_BLOCK_SIZE);
                    }
                })
                .flatMap(new Func1<Long, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(final Long position) {
                        return sfsDataV1.getDataStream(sfsVertx, position, Optional.absent(), Optional.absent())
                                .map(new Func1<Optional<ReadStreamBlob>, Long>() {
                                    @Override
                                    public Long call(Optional<ReadStreamBlob> headerBlobOptional) {
                                        VertxAssert.assertFalse(context, headerBlobOptional.isPresent());
                                        return position;
                                    }
                                });
                    }
                })
                .flatMap(new Func1<Long, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(final Long position) {
                        return sfsDataV1.acknowledge(sfsVertx, position)
                                .map(new Func1<Optional<HeaderBlob>, Long>() {
                                    @Override
                                    public Long call(Optional<HeaderBlob> headerBlobOptional) {
                                        VertxAssert.assertFalse(context, headerBlobOptional.isPresent());
                                        return position;
                                    }
                                });
                    }
                })
                .flatMap(new Func1<Long, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(final Long position) {
                        return sfsDataV1.delete(sfsVertx, position)
                                .map(new Func1<Optional<HeaderBlob>, Long>() {
                                    @Override
                                    public Long call(Optional<HeaderBlob> headerBlobOptional) {
                                        VertxAssert.assertFalse(context, headerBlobOptional.isPresent());
                                        return position;
                                    }
                                });
                    }
                })
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, sfsDataV1))
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testVolumeCopy(TestContext context) throws IOException {
        ExecutorService backgroundPool = Executors.newFixedThreadPool(200);
        ExecutorService ioPool = Executors.newFixedThreadPool(200);
        SfsVertx sfsVertx = new SfsVertxImpl(rule.vertx(), backgroundPool, ioPool);

        final Buffer expectedBuffer = Buffer.buffer("HELLO");
        final VolumeV1 sfsDataV1 = new VolumeV1(path);

        Path tempPath = Files.createTempDirectory("");

        Async async = context.async();
        sfsDataV1.open(sfsVertx)
                .flatMap(new PutFile<Void>(context, sfsVertx, sfsDataV1, expectedBuffer, 0L))
                .flatMap(new GetFile(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new FileHash(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new FileSize(context, sfsVertx, sfsDataV1, expectedBuffer))
                .flatMap(new AckFile(context, sfsVertx, sfsDataV1))
                .flatMap(new Func1<Long, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(Long position) {
                        return sfsDataV1.copy(sfsVertx, tempPath)
                                .flatMap(new Func1<Void, Observable<Long>>() {
                                    @Override
                                    public Observable<Long> call(Void aVoid) {
                                        VolumeV1 clonedVolume = new VolumeV1(tempPath);
                                        return clonedVolume.open(sfsVertx)
                                                .map(new Func1<Void, Long>() {
                                                    @Override
                                                    public Long call(Void bVoid) {
                                                        return position;
                                                    }
                                                })
                                                .flatMap(new GetFile(context, sfsVertx, clonedVolume, expectedBuffer))
                                                .flatMap(new Func1<Long, Observable<Long>>() {
                                                    @Override
                                                    public Observable<Long> call(Long position) {
                                                        return clonedVolume.close(sfsVertx)
                                                                .map(aVoid1 -> position);
                                                    }
                                                });
                                    }
                                });
                    }
                })
                .map(new ToVoid<Long>())
                .flatMap(new Stop(sfsVertx, sfsDataV1))
                .subscribe(new TestSubscriber(context, async));
    }

}