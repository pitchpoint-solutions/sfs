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

package org.sfs.io;

import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;
import org.sfs.SfsVertx;
import org.sfs.encryption.Algorithm;
import org.sfs.encryption.AlgorithmDef;
import org.sfs.rx.RxHelper;
import rx.Observable;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.nio.channels.AsynchronousFileChannel.open;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Arrays.fill;
import static org.sfs.encryption.AlgorithmDef.getPreferred;
import static org.sfs.rx.Defer.aVoid;

public class FileBackedBuffer implements BufferEndableWriteStream {

    private static final Logger LOGGER = getLogger(FileBackedBuffer.class);
    private static final int MAX_WRITES = 16 * 1024;
    private static final AlgorithmDef ALGORITHM_DEF = getPreferred();
    private static byte[] SALT = ALGORITHM_DEF.generateSaltBlocking();
    private int fileThreshold;
    private SfsVertx sfsVertx;
    private Path tempFileDirectory;
    private Path tempFile;
    private AsynchronousFileChannel channel;
    private boolean fileOpen = false;
    private long size = 0;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> endHandler;
    private Handler<Void> drainHandler;
    private Buffer memory;
    private BufferEndableWriteStream fileWriteStreamConsumer;
    private CountingEndableWriteStream countingEndableWriteStream;
    private Algorithm algorithm;
    private boolean openedReadStream = false;
    private boolean ended = false;
    private boolean encryptTempFile;
    private Context context;

    public FileBackedBuffer(SfsVertx sfsVertx, int fileThreshold, boolean encryptTempFile, Path tempFileDirectory) {
        this.sfsVertx = sfsVertx;
        this.encryptTempFile = encryptTempFile;
        this.fileThreshold = fileThreshold;
        this.memory = buffer();
        this.tempFileDirectory = tempFileDirectory;
        this.context = sfsVertx.getOrCreateContext();
    }

    public FileBackedBuffer(SfsVertx sfsVertx, int fileThreshold, Path tempFileDirectory) {
        this(sfsVertx, fileThreshold, true, tempFileDirectory);
    }

    public boolean isFileOpen() {
        return fileOpen;
    }

    public long length() {
        return size;
    }

    public ReadStream<Buffer> readStream() {
        checkReadStreamNotOpen();
        openedReadStream = true;
        if (fileOpen) {
            AsyncFileReader asyncFileReader = new AsyncFileReaderImpl(context, 0, 8192, countingEndableWriteStream.count(), channel, LOGGER);
            if (encryptTempFile) {
                return algorithm.decrypt(asyncFileReader);
            } else {
                return asyncFileReader;
            }
        } else {
            return new BufferReadStream(memory);
        }
    }

    @Override
    public BufferEndableWriteStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        BufferEndableWriteStream dst = maybeGetWriteStream();
        if (dst != null) {
            dst.endHandler(endHandler);
        } else {
            handleEnd();
        }
        return this;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        BufferEndableWriteStream dst = maybeGetWriteStream();
        if (dst != null) {
            dst.exceptionHandler(handler);
        }
        return this;
    }

    @Override
    public WriteStream<Buffer> write(Buffer data) {
        checkReadStreamNotOpen();
        size += data.length();
        BufferEndableWriteStream dst = maybeGetWriteStream();
        if (dst != null) {
            dst.write(data);
        } else {
            memory.appendBuffer(data);
            handleDrain();
        }
        return this;
    }

    @Override
    public void end() {
        checkReadStreamNotOpen();
        checkState(!ended, "Already ended");
        ended = true;
        BufferEndableWriteStream dst = maybeGetWriteStream();
        if (dst != null) {
            dst.end();
        } else {
            handleEnd();
        }
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        BufferEndableWriteStream dst = maybeGetWriteStream();
        if (dst != null) {
            return dst.writeQueueFull();
        } else {
            return false;
        }
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> drainHandler) {
        this.drainHandler = drainHandler;
        BufferEndableWriteStream dst = maybeGetWriteStream();
        if (dst != null) {
            dst.drainHandler(drainHandler);
        } else {
            handleDrain();
        }
        return this;
    }

    @Override
    public void end(Buffer data) {
        checkReadStreamNotOpen();
        size += data.length();
        checkState(!ended, "Already ended");
        ended = true;
        BufferEndableWriteStream dst = maybeGetWriteStream();
        if (dst != null) {
            dst.end(data);
        } else {
            memory.appendBuffer(data);
            handleEnd();
        }
    }

    public Observable<Void> close() {
        if (channel != null || tempFile != null) {
            return RxHelper.executeBlocking(context, sfsVertx.getBackgroundPool(), () -> {
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (IOException e) {
                        LOGGER.warn("Handling close exception", e);
                    }
                }
                if (tempFile != null) {
                    try {
                        deleteIfExists(tempFile);
                    } catch (IOException e) {
                        LOGGER.warn("Handling close exception", e);
                    }
                }
                return (Void) null;
            });
        } else {
            return aVoid();
        }
    }

    protected void handleError(Throwable e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
        } else {
            LOGGER.error("Unhandled Exception", e);
        }
    }

    protected void handleDrain() {
        Handler<Void> handler = drainHandler;
        if (handler != null) {
            drainHandler = null;
            handler.handle(null);
        }
    }

    protected void handleEnd() {
        if (ended) {
            Handler<Void> handler = endHandler;
            if (handler != null) {
                endHandler = null;
                handler.handle(null);
            }
        }
    }


    protected BufferEndableWriteStream maybeGetWriteStream() {
        if (size > fileThreshold) {
            if (!fileOpen) {
                fileOpen = true;
                try {
                    Set<StandardOpenOption> options = new HashSet<>();
                    options.add(READ);
                    options.add(WRITE);
                    tempFile = createTempFile(tempFileDirectory, "FileBackedBufferWriteStreamConsumer", "");
                    channel =
                            open(
                                    tempFile,
                                    options,
                                    sfsVertx.getIoPool());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                WriteQueueSupport<AsyncFileWriter> writeQueueSupport = new WriteQueueSupport<>(MAX_WRITES);
                fileWriteStreamConsumer = new AsyncFileWriterImpl(0L, writeQueueSupport, context, channel, LOGGER);
                countingEndableWriteStream = new CountingEndableWriteStream(fileWriteStreamConsumer);
                fileWriteStreamConsumer = countingEndableWriteStream;
                if (encryptTempFile) {
                    byte[] secret = ALGORITHM_DEF.generateKeyBlocking();
                    try {
                        algorithm = ALGORITHM_DEF.create(secret, SALT);
                        fileWriteStreamConsumer = algorithm.encrypt(fileWriteStreamConsumer);
                    } finally {
                        fill(secret, (byte) 0);
                    }
                }
                fileWriteStreamConsumer.exceptionHandler(exceptionHandler);
                if (memory.length() > 0) {
                    fileWriteStreamConsumer.write(memory);
                }
                fileWriteStreamConsumer.drainHandler(drainHandler);
                fileWriteStreamConsumer.endHandler(endHandler);
            }
            return fileWriteStreamConsumer;
        } else {
            return null;
        }
    }

    protected void checkReadStreamNotOpen() {
        checkArgument(!openedReadStream, "ReadStream is open");
    }
}
