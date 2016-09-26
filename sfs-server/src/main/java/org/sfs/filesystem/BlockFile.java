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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;
import org.sfs.block.RangeLock;
import org.sfs.io.AsyncFileReader;
import org.sfs.io.AsyncFileReaderImpl;
import org.sfs.io.AsyncFileWriter;
import org.sfs.io.AsyncFileWriterImpl;
import org.sfs.io.Block;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.BufferWriteEndableWriteStream;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.FluentIterable.from;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Integer.MAX_VALUE;
import static java.nio.file.Files.createDirectories;
import static java.util.Collections.addAll;
import static org.sfs.block.RangeLock.Lock;
import static org.sfs.filesystem.BlockFile.Status.STARTED;
import static org.sfs.filesystem.BlockFile.Status.STARTING;
import static org.sfs.filesystem.BlockFile.Status.STOPPED;
import static org.sfs.filesystem.BlockFile.Status.STOPPING;
import static org.sfs.io.AsyncIO.end;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.io.Block.decodeFrame;
import static org.sfs.io.Block.encodeFrame;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.Buffers.partition;
import static rx.Observable.create;
import static rx.Observable.defer;
import static rx.Observable.just;
import static rx.Observable.using;

public class BlockFile {

    enum Status {
        STARTING,
        STARTED,
        STOPPING,
        STOPPED
    }

    private static final Logger LOGGER = getLogger(BlockFile.class);
    private static final int MAX_WRITES = 16 * 1024;
    private final Path file;
    private final int blockSize;
    private final RangeLock lock;
    private AsynchronousFileChannel channel;
    private final WriteQueueSupport writeQueueSupport = new WriteQueueSupport(MAX_WRITES);
    private final Set<BufferEndableWriteStream> activeWriters = new ConcurrentHashSet<>();
    private AtomicBoolean readOnly = new AtomicBoolean(true);
    private final AtomicReference<Status> status = new AtomicReference<>(STOPPED);
    private ExecutorService executorService;

    public BlockFile(Path file, int blockSize) {
        this.file = file;
        this.blockSize = blockSize;
        this.lock = new RangeLock(blockSize);
    }

    public int getBlockSize() {
        return blockSize;
    }

    public Observable<Void> open(SfsVertx vertx, StandardOpenOption openOption, StandardOpenOption... openOptions) {
        executorService = vertx.getIoPool();
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
                .doOnNext(aVoid -> checkState(status.compareAndSet(STARTING, STARTED)));
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
                .doOnNext(aVoid -> checkState(status.compareAndSet(STARTED, STOPPING)))
                .doOnNext(aVoid -> readOnly.compareAndSet(false, true))
                .flatMap(new WaitForActiveWriters(vertx, activeWriters))
                .flatMap(new WaitForEmptyWriteQueue(vertx, writeQueueSupport))
                .flatMap(aVoid -> executeBlocking(vertx, () -> {
                    try {
                        channel.close();
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

    public Observable<Iterable<ChecksummedPositional<byte[]>>> getBlocks(SfsVertx vertx, final long position,
                                                                         int numberOfBlocks) {
        return defer(() -> {
            checkOpen();
            long bufferSize = blockSize * numberOfBlocks;
            checkState(bufferSize <= MAX_VALUE, "Overflow multiplying %s and %s", blockSize, numberOfBlocks);
            AsyncFileReader src = createReadStream(vertx, position, (int) bufferSize, bufferSize);
            BufferWriteEndableWriteStream dst = new BufferWriteEndableWriteStream();
            return pump(src, dst)
                    .map(aVoid -> {
                        Buffer buffer = dst.toBuffer();
                        Positional<Buffer> bulk = new Positional<>(position, buffer);
                        Iterable<Positional<Buffer>> buffers = partition(bulk, blockSize);
                        return from(buffers)
                                .transform(positional -> {
                                    Optional<Block.Frame<byte[]>> oFrame = decodeFrame(positional.getValue(), false);
                                    if (oFrame.isPresent()) {
                                        Block.Frame<byte[]> frame = oFrame.get();
                                        return of(new ChecksummedPositional<byte[]>(positional.getPosition(), frame.getData(), frame.getChecksum()) {
                                            @Override
                                            public boolean isChecksumValid() {
                                                return frame.isChecksumValid();
                                            }
                                        });
                                    } else {
                                        return Optional.<ChecksummedPositional<byte[]>>absent();
                                    }
                                })
                                .filter(Optional::isPresent)
                                .transform(Optional::get);
                    });
        });
    }

    public Observable<Optional<ChecksummedPositional<byte[]>>> getBlock(SfsVertx vertx, final long position) {
        return getBlock0(vertx, position)
                .map(buffer -> {
                    Optional<Block.Frame<byte[]>> oFrame = decodeFrame(buffer, false);
                    if (oFrame.isPresent()) {
                        Block.Frame<byte[]> frame = oFrame.get();
                        return of(new ChecksummedPositional<byte[]>(position, frame.getData(), frame.getChecksum()) {
                            @Override
                            public boolean isChecksumValid() {
                                return frame.isChecksumValid();
                            }
                        });
                    } else {
                        return absent();
                    }
                });
    }

    public Observable<Void> setBlock(SfsVertx vertx, final long position, final Buffer data) {
        return defer(() -> {
            checkOpen();
            checkCanWrite();
            Block.Frame<Buffer> frame = encodeFrame(data);
            Buffer frameBuffer = frame.getData();
            return setBlock0(vertx, position, frameBuffer);
        });
    }

    public Observable<Boolean> replaceBlock(SfsVertx vertx, final long position, Buffer oldValue, Buffer newValue) {
        return defer(() -> {
            checkOpen();
            checkCanWrite();
            return getBlock0(vertx, position)
                    .flatMap(buffer -> {
                        Optional<Block.Frame<byte[]>> oExistingValue = decodeFrame(buffer, false);
                        if (oExistingValue.isPresent()) {
                            Block.Frame<byte[]> existingValue = oExistingValue.get();
                            if (Arrays.equals(existingValue.getData(), oldValue.getBytes())) {
                                Block.Frame<Buffer> frame = encodeFrame(newValue);
                                Buffer frameBuffer = frame.getData();
                                return setBlock0(vertx, position, frameBuffer)
                                        .map(aVoid -> true);
                            } else {
                                return just(false);
                            }
                        } else {
                            Block.Frame<Buffer> frame = encodeFrame(newValue);
                            Buffer frameBuffer = frame.getData();
                            return setBlock0(vertx, position, frameBuffer)
                                    .map(aVoid -> true);
                        }
                    });
        });
    }

    private Observable<Buffer> getBlock0(SfsVertx vertx, final long position) {
        AsyncFileReader src = createReadStream(vertx, position, blockSize, blockSize);
        BufferWriteEndableWriteStream dst = new BufferWriteEndableWriteStream();
        return pump(src, dst)
                .map(aVoid -> dst.toBuffer());
    }


    private Observable<Void> setBlock0(SfsVertx vertx, final long position, Buffer data) {
        long length = data.length();
        // this should never happen but in case things ever get crazy this will prevent corruption
        checkState(length <= blockSize, "Frame size was %s, expected %s", length, blockSize);

        return using(
                () -> {
                    AsyncFileWriter writer = createWriteStream(vertx, position);
                    activeWriters.add(writer);
                    return writer;
                },
                writer ->
                        end(data, writer)
                                .doOnNext(aVoid -> activeWriters.remove(writer)),
                activeWriters::remove);
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

    protected AsyncFileReader createReadStream(SfsVertx vertx, final long startPosition,
                                               int bufferSize, long length) {
        checkAligned(startPosition, blockSize);
        checkAligned(bufferSize, blockSize);
        AsyncFileReader reader = new AsyncFileReaderImpl(
                vertx, startPosition,
                bufferSize,
                length,
                channel,
                LOGGER);
        return reader;
    }

    protected AsyncFileWriter createWriteStream(SfsVertx vertx, long startPosition) {
        checkAligned(startPosition, blockSize);
        AsyncFileWriter writer =
                new AsyncFileWriterImpl(
                        startPosition,
                        writeQueueSupport,
                        vertx,
                        channel,
                        LOGGER);
        return writer;
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
}
