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


import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncFileWriterImpl implements AsyncFileWriter {

    private final Logger log;

    private final AsynchronousFileChannel ch;
    private final Context context;

    private Handler<Throwable> exceptionHandler;

    private WriteQueueSupport<AsyncFileWriter> writeQueueSupport;

    private Handler<Void> endHandler;

    private long startPosition;
    private long writePos;
    private long lastWriteTime;
    private boolean ended = false;

    public AsyncFileWriterImpl(long startPosition, WriteQueueSupport<AsyncFileWriter> writeQueueSupport, Context context, AsynchronousFileChannel dataFile, Logger log) {
        this.log = log;
        this.startPosition = startPosition;
        this.writePos = startPosition;
        this.ch = dataFile;
        this.context = context;
        this.writeQueueSupport = writeQueueSupport;
        this.lastWriteTime = System.currentTimeMillis();
    }

    protected void checkNotEnded() {
        Preconditions.checkState(!ended, "WriteStream ended");
    }

    @Override
    public boolean isEnded() {
        return ended;
    }

    @Override
    public AsyncFileWriterImpl endHandler(Handler<Void> endHandler) {
        checkNotEnded();
        this.endHandler = endHandler;
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        checkNotEnded();
        ended = true;
        int length = buffer.length();
        doWrite(buffer, writePos, event -> {
            if (event.failed()) {
                handleException(event.cause());
            } else {
                handleEnd();
            }
        });
        writePos += length;
        lastWriteTime = System.currentTimeMillis();
    }

    @Override
    public void end() {
        checkNotEnded();
        ended = true;
        handleEnd();
    }

    @Override
    public long writePosition() {
        return writePos;
    }

    @Override
    public long startPosition() {
        return startPosition;
    }

    @Override
    public long lastWriteTime() {
        return lastWriteTime;
    }

    @Override
    public AsyncFileWriterImpl write(Buffer buffer) {
        checkNotEnded();
        int length = buffer.length();
        doWrite(buffer, writePos, event -> {
            if (event.failed()) {
                handleException(event.cause());
            } else {
                handleEnd();
            }
        });
        writePos += length;
        lastWriteTime = System.currentTimeMillis();
        return this;
    }

    private AsyncFileWriterImpl doWrite(Buffer buffer, long position, Handler<AsyncResult<Void>> handler) {
        Preconditions.checkNotNull(buffer, "buffer");
        Preconditions.checkArgument(position >= 0, "position must be >= 0");
        Handler<AsyncResult<Void>> wrapped = ar -> {
            if (ar.succeeded()) {
                if (handler != null) {
                    handler.handle(ar);
                }
            } else {
                if (handler != null) {
                    handler.handle(ar);
                } else {
                    handleException(ar.cause());
                }
            }
        };
        ByteBuf buf = buffer.getByteBuf();
        if (buf.nioBufferCount() > 1) {
            doWrite(buf.nioBuffers(), position, wrapped);
        } else {
            ByteBuffer bb = buf.nioBuffer();
            doWrite(bb, position, bb.limit(), wrapped);
        }
        return this;
    }

    private void doWrite(ByteBuffer[] buffers, long position, Handler<AsyncResult<Void>> handler) {
        AtomicInteger cnt = new AtomicInteger();
        AtomicBoolean sentFailure = new AtomicBoolean();
        for (ByteBuffer b : buffers) {
            int limit = b.limit();
            doWrite(b, position, limit, ar -> {
                if (ar.succeeded()) {
                    if (cnt.incrementAndGet() == buffers.length) {
                        handler.handle(ar);
                    }
                } else {
                    if (sentFailure.compareAndSet(false, true)) {
                        handler.handle(ar);
                    }
                }
            });
            position += limit;
        }
    }

    private void doWrite(ByteBuffer buff, long position, int toWrite, Handler<AsyncResult<Void>> handler) {
        if (toWrite <= 0) {
            handler.handle(Future.succeededFuture());
        } else {
            incrementWritesOutstanding(toWrite);
            writeInternal(buff, position, handler::handle);
        }
    }

    @Override
    public AsyncFileWriterImpl setWriteQueueMaxSize(int maxSize) {
        // do nothing
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return writeQueueSupport.writeQueueFull();
    }

    @Override
    public boolean writeQueueEmpty() {
        return writeQueueSupport.writeQueueEmpty(this);
    }

    public void incrementWritesOutstanding(int delta) {
        writeQueueSupport.incrementWritesOutstanding(this, delta);
    }

    public void decrementWritesOutstanding(int delta) {
        writeQueueSupport.decrementWritesOutstanding(this, delta);
    }

    protected void removeWritesOutstandingCounter() {
        writeQueueSupport.remove(this);
    }

    @Override
    public AsyncFileWriterImpl drainHandler(Handler<Void> handler) {
        writeQueueSupport.drainHandler(context, handler);
        return this;
    }

    @Override
    public AsyncFileWriterImpl exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    private void handleException(Throwable t) {
        if (exceptionHandler != null && t instanceof Exception) {
            exceptionHandler.handle(t);
        } else {
            log.error("Unhandled exception", t);

        }
    }

    private void handleEnd() {
        if (ended) {
            Handler<Void> h = endHandler;
            if (h != null) {
                endHandler = null;
                writeQueueSupport.emptyHandler(this, context, h);
            }
        }
    }

    private void writeInternal(ByteBuffer buff, long position, Handler<AsyncResult<Void>> handler) {
        try {
            ch.write(buff, position, null, new java.nio.channels.CompletionHandler<Integer, Object>() {

                public void completed(Integer bytesWritten, Object attachment) {

                    long pos = position;

                    if (buff.hasRemaining()) {
                        // partial write
                        pos += bytesWritten;
                        // resubmit
                        writeInternal(buff, pos, handler);
                    } else {
                        // It's been fully written
                        context.runOnContext((v) -> {
                            decrementWritesOutstanding(buff.limit());
                            handler.handle(Future.succeededFuture());
                        });
                    }
                }

                public void failed(Throwable exc, Object attachment) {
                    context.runOnContext(v -> {
                        removeWritesOutstandingCounter();
                        handler.handle(Future.failedFuture(exc));
                    });
                }
            });
        } catch (RuntimeException e) {
            context.runOnContext(v -> {
                removeWritesOutstandingCounter();
                handler.handle(Future.failedFuture(e));
            });
        }
    }
}