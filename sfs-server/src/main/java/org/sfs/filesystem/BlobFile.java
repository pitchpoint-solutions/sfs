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

package org.sfs.filesystem;

import com.google.common.base.Optional;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.sfs.SfsVertx;
import org.sfs.block.RangeLock;
import org.sfs.io.AsyncFileReader;
import org.sfs.io.AsyncFileReaderImpl;
import org.sfs.io.AsyncFileWriter;
import org.sfs.io.AsyncFileWriterImpl;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.BufferedEndableWriteStream;
import org.sfs.io.LimitedReadStream;
import org.sfs.io.LimitedWriteEndableWriteStream;
import org.sfs.io.WaitForActiveWriters;
import org.sfs.io.WaitForEmptyWriteQueue;
import org.sfs.io.WriteQueueSupport;
import org.sfs.rx.ResultMemoizeHandler;
import rx.Observable;
import rx.functions.Func0;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.System.currentTimeMillis;
import static java.nio.file.Files.createDirectories;
import static java.util.Collections.addAll;
import static org.sfs.block.RangeLock.Lock;
import static org.sfs.filesystem.BlobFile.Status.STARTED;
import static org.sfs.filesystem.BlobFile.Status.STARTING;
import static org.sfs.filesystem.BlobFile.Status.START_FAILED;
import static org.sfs.filesystem.BlobFile.Status.STOPPED;
import static org.sfs.filesystem.BlobFile.Status.STOPPING;
import static org.sfs.io.AsyncIO.end;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.empty;
import static rx.Observable.create;
import static rx.Observable.defer;
import static rx.Observable.error;
import static rx.Observable.using;

public class BlobFile {

    enum Status {
        STARTING,
        STARTED,
        START_FAILED,
        STOPPING,
        STOPPED
    }

    private static final Logger LOGGER = getLogger(BlobFile.class);
    private static final int MAX_WRITES = 16 * 1024;
    private Vertx vertx;
    private final long writeStreamTimeout;
    private final Path file;
    private final int blockSize;
    private final int produceBufferSize;
    private final RangeLock lock;
    private AsynchronousFileChannel channel;
    private final WriteQueueSupport writeQueueSupport = new WriteQueueSupport(MAX_WRITES);
    private final Set<AsyncFileWriter> activeWriters = new ConcurrentHashSet<>();
    private AtomicBoolean readOnly = new AtomicBoolean(true);
    private Set<Long> periodics = new ConcurrentHashSet<>();
    private final AtomicReference<Status> status = new AtomicReference<>(STOPPED);
    private ExecutorService executorService;

    public BlobFile(Path file, int blockSize, long writeStreamTimeout) {
        this.file = file;
        this.blockSize = blockSize;
        this.produceBufferSize = blockSize * 10000;
        this.lock = new RangeLock(blockSize);
        this.writeStreamTimeout = writeStreamTimeout;
    }

    public Status getStatus() {
        return status.get();
    }

    public int getBlockSize() {
        return blockSize;
    }

    public Observable<Void> open(SfsVertx vertx, StandardOpenOption openOption, StandardOpenOption... openOptions) {
        this.vertx = vertx;
        this.executorService = vertx.getIoPool();
        return empty()
                .doOnNext(aVoid -> checkState(status.compareAndSet(STOPPED, STARTING)))
                .flatMap(aVoid -> executeBlocking(vertx, () -> {
                    try {
                        createDirectories(file.getParent());

                        Set<StandardOpenOption> options = new HashSet<>();
                        options.add(openOption);
                        addAll(options, openOptions);

                        channel =
                                AsynchronousFileChannel.open(
                                        file,
                                        options,
                                        executorService);

                        return (Void) null;

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .doOnNext(aVoid -> {
                    long id = vertx.setPeriodic(100, event -> cleanupOrphanedWriters());
                    periodics.add(id);
                })
                .doOnNext(aVoid -> checkState(status.compareAndSet(STARTING, STARTED)))
                .onErrorResumeNext(throwable -> {
                    checkState(status.compareAndSet(STARTING, START_FAILED));
                    return error(throwable);
                });
    }

    public long getWriteQueueLowWater() {
        return writeQueueSupport.getLowWater();
    }

    public long getWriteQueueMaxWrites() {
        return writeQueueSupport.getMaxWrites();
    }

    public long getWriteQueueSize() {
        return writeQueueSupport.getSize();
    }

    public int getLockCount() {
        return lock.getLockCount();
    }

    public Optional<Lock> tryWriteLock(long position, long length) {
        return lock.tryWriteLock(position, length);
    }

    public Optional<Lock> tryReadLock(long position, long length) {
        return lock.tryReadLock(position, length);
    }


    public Observable<Void> disableWrites(SfsVertx vertx) {
        return empty()
                .doOnNext(aVoid -> checkOpen())
                .doOnNext(aVoid -> readOnly.compareAndSet(false, true))
                .flatMap(new WaitForActiveWriters(vertx, activeWriters))
                .flatMap(new WaitForEmptyWriteQueue(vertx, writeQueueSupport));
    }

    public Observable<Void> enableWrites(SfsVertx vertx) {
        return empty()
                .doOnNext(aVoid -> checkOpen())
                .doOnNext(aVoid -> readOnly.compareAndSet(true, false));
    }

    public Observable<Void> close(SfsVertx vertx) {
        return empty()
                .doOnNext(aVoid -> checkState(status.compareAndSet(STARTED, STOPPING) || status.compareAndSet(START_FAILED, STOPPING), "Status was %s expected %s or %s", status.get(), STARTED, START_FAILED))
                .doOnNext(aVoid -> readOnly.compareAndSet(false, true))
                .flatMap(new WaitForActiveWriters(vertx, activeWriters))
                .flatMap(new WaitForEmptyWriteQueue(vertx, writeQueueSupport))
                .doOnNext(aVoid -> periodics.forEach(vertx::cancelTimer))
                .flatMap(aVoid -> executeBlocking(vertx, () -> {
                    try {
                        if (channel != null) {
                            channel.close();
                        }
                        return (Void) null;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .doOnNext(aVoid -> checkState(status.compareAndSet(STOPPING, STOPPED)));
    }

    public Observable<Long> size(SfsVertx vertx) {
        return empty()
                .doOnNext(aVoid -> checkOpen())
                .flatMap(aVoid -> executeBlocking(vertx, () -> {
                    try {
                        checkNotNull(channel, "Channel is null. Was everything initialized??");
                        return channel.size();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    public Observable<Void> copy(SfsVertx vertx, BlobFile srcBlobFile, long srcPosition, long srcLength, long dstPosition, long dstLength) {
        return defer(() -> {
            srcBlobFile.checkOpen();
            checkOpen();
            checkCanWrite();
            AsyncFileReader src = srcBlobFile.createReadStream(vertx, srcPosition, produceBufferSize, srcLength);
            LimitedReadStream value = new LimitedReadStream(src, srcLength);
            return consume(vertx, dstPosition, dstLength, value);
        });
    }

    public Observable<Void> copy(SfsVertx vertx, long srcPosition, long srcLength, BlobFile dstBlobFile, long dstPosition, long dstLength) {
        return defer(() -> {
            checkOpen();
            dstBlobFile.checkOpen();
            dstBlobFile.checkCanWrite();
            LimitedWriteEndableWriteStream limitedWriteStream = new LimitedWriteEndableWriteStream(new BufferedEndableWriteStream(dstBlobFile.createWriteStream(vertx, dstPosition, true)), dstLength);
            return produce(vertx, srcPosition, srcLength, limitedWriteStream);
        });
    }

    public Observable<Void> produce(SfsVertx vertx, long position, long length, BufferEndableWriteStream dst) {
        return defer(() -> {
            checkOpen();
            AsyncFileReader src = createReadStream(vertx, position, produceBufferSize, length);
            LimitedReadStream value = new LimitedReadStream(src, length);
            return pump(value, dst);
        });
    }


    public Observable<Void> consume(SfsVertx vertx, long position, long length, ReadStream<Buffer> src, boolean assertAlignment) {
        return defer(() -> {
            checkOpen();
            checkCanWrite();
            return using(
                    () -> {
                        AsyncFileWriter dst = createWriteStream(vertx, position, assertAlignment);
                        activeWriters.add(dst);
                        return dst;
                    },
                    sfsWriteStream -> {
                        BufferedEndableWriteStream bufferedWriteStream = new BufferedEndableWriteStream(sfsWriteStream);
                        LimitedWriteEndableWriteStream limitedWriteStream = new LimitedWriteEndableWriteStream(bufferedWriteStream, length);
                        return pump(src, limitedWriteStream)
                                .doOnNext(aVoid -> activeWriters.remove(sfsWriteStream));
                    },
                    activeWriters::remove
            );
        });
    }

    public Observable<Void> consume(SfsVertx vertx, long position, long length, ReadStream<Buffer> src) {
        return consume(vertx, position, length, src, true);
    }

    public Observable<Void> consume(SfsVertx vertx, long position, Buffer src) {
        return consume(vertx, position, src, true);
    }

    public Observable<Void> consume(SfsVertx vertx, long position, Buffer src, boolean assertAlignment) {
        return defer(() -> {
            checkOpen();
            checkCanWrite();
            return using(
                    () -> {
                        AsyncFileWriter dst = createWriteStream(vertx, position, assertAlignment);
                        activeWriters.add(dst);
                        return dst;
                    },
                    sfsWriteStream -> {
                        LimitedWriteEndableWriteStream limitedWriteStream = new LimitedWriteEndableWriteStream(sfsWriteStream, src.length());
                        return end(src, limitedWriteStream)
                                .doOnNext(aVoid -> activeWriters.remove(sfsWriteStream));
                    },
                    activeWriters::remove
            );
        });

    }

    protected void cleanupOrphanedWriters() {
        long now = currentTimeMillis();
        activeWriters.stream().filter(sfsWriteStream -> now - sfsWriteStream.lastWriteTime() >= writeStreamTimeout)
                .forEach(activeWriters::remove);

    }

    protected AsyncFileReader createReadStream(SfsVertx vertx, final long startPosition, int bufferSize, long length) {
        AsyncFileReader reader = new AsyncFileReaderImpl(
                vertx,
                startPosition,
                bufferSize,
                length,
                channel,
                LOGGER);
        return reader;
    }

    protected AsyncFileWriter createWriteStream(SfsVertx vertx, long startPosition, boolean assertAlignment) {
        if (assertAlignment) {
            checkAligned(startPosition, blockSize);
        }
        AsyncFileWriter writer =
                new AsyncFileWriterImpl(
                        startPosition,
                        writeQueueSupport,
                        vertx,
                        channel,
                        LOGGER);

        return writer;
    }

    public Observable<Void> force(SfsVertx vertx, boolean metaData) {
        return executeBlocking(vertx, () -> {
            checkOpen();
            try {
                channel.force(metaData);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return (Void) null;
        });
    }

    protected void checkAligned(long value, int blockSize) {
        checkState(value % blockSize == 0, "%s is not multiple of %s", value, blockSize);
    }

    protected void checkOpen() {
        Status s = status.get();
        checkState(STARTED.equals(s), "Not open. Status was %s", s);
    }

    protected void checkCanWrite() {
        checkState(!readOnly.get(), "ReadOnly mode is set");
    }

    protected <T> Observable<T> executeBlocking(SfsVertx vertx, Func0<T> func0) {
        Context context = vertx.getOrCreateContext();
        ResultMemoizeHandler<T> handler = new ResultMemoizeHandler<T>();
        executorService.execute(() -> {
            try {
                T result = func0.call();
                context.runOnContext(event -> handler.complete(result));
            } catch (Throwable e) {
                context.runOnContext(event -> handler.fail(e));
            }
        });
        return create(handler.subscribe);
    }
}
