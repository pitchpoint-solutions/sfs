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
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;

public class AsyncFileReaderImpl implements AsyncFileReader {

    private final Logger log;

    private final AsynchronousFileChannel ch;
    private final Context context;
    private Handler<Throwable> exceptionHandler;

    private boolean paused = false;
    private Handler<Buffer> dataHandler;
    private Handler<Void> endHandler;
    private long readPos;
    private final int bufferSize;
    private final long startPosition;
    private long bytesRemaining;
    private boolean ended = false;

    public AsyncFileReaderImpl(Context context, long startPosition, int bufferSize, long length, AsynchronousFileChannel dataFile, Logger log) {
        this.log = log;
        this.bufferSize = bufferSize;
        this.readPos = startPosition;
        this.bytesRemaining = length;
        this.startPosition = startPosition;
        this.ch = dataFile;
        this.context = context;
    }

    @Override
    public AsyncFileReaderImpl fetch(long amount) {
        return this;
    }

    @Override
    public long startPosition() {
        return startPosition;
    }

    @Override
    public long readPosition() {
        return readPos;
    }

    @Override
    public int bufferSize() {
        return bufferSize;
    }

    @Override
    public AsyncFileReaderImpl exceptionHandler(Handler<Throwable> handler) {
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

    private AsyncFileReaderImpl read(Buffer buffer, int offset, long position, int length, Handler<AsyncResult<Result>> handler) {
        try {
            ByteBuffer bb = ByteBuffer.allocate(length);
            doRead(buffer, offset, bb, position, position, handler);
            return this;
        } catch (Throwable e) {
            handleException(e);
            return this;
        }
    }

    private void doRead() {
        checkNotEnded();
        try {
            Handler<AsyncResult<Result>> handler = ar -> {
                try {
                    if (ar.succeeded()) {
                        Result result = ar.result();
                        Buffer buffer = result.buffer;
                        int bufferLength = buffer.length();
                        bytesRemaining -= bufferLength;
                        readPos += bufferLength;
                        Preconditions.checkState(bytesRemaining >= 0, "bytes remaining was %s, expected >= 0", bytesRemaining);
                        if (buffer.length() <= 0) {
                            ended = true;
                            handleEnd();
                        } else {
                            handleData(buffer);
                            if (!paused && dataHandler != null) {
                                doRead();
                            }
                        }
                    } else {
                        ended = true;
                        handleException(ar.cause());
                    }
                } catch (Throwable e) {
                    ended = true;
                    handleException(e);
                }
            };
            int countOfBytesToRead = (int) Math.min(bytesRemaining, bufferSize);
            Buffer buff = Buffer.buffer(countOfBytesToRead);
            read(buff, 0, readPos, countOfBytesToRead, handler);
        } catch (Throwable e) {
            ended = true;
            handleException(e);
        }
    }

    @Override
    public AsyncFileReaderImpl handler(Handler<Buffer> handler) {
        checkNotEnded();
        this.dataHandler = handler;
        if (!paused && dataHandler != null) {
            doRead();
        }
        return this;
    }

    @Override
    public AsyncFileReaderImpl endHandler(Handler<Void> handler) {
        this.endHandler = handler;
        handleEnd();
        return this;
    }

    @Override
    public AsyncFileReaderImpl pause() {
        paused = true;
        return this;
    }

    @Override
    public AsyncFileReaderImpl resume() {
        if (!isEnded()) {
            if (paused && dataHandler != null) {
                paused = false;
                doRead();
            }
        }
        return this;
    }

    @Override
    public boolean isEnded() {
        return ended;
    }

    private void handleData(Buffer buffer) {
        Preconditions.checkState(dataHandler != null, "Handler must be set");
        dataHandler.handle(buffer);
    }

    private void handleEnd() {
        if (ended) {
            Handler<Void> h = endHandler;
            if (h != null) {
                endHandler = null;
                h.handle(null);
            }
        }
    }

    private void checkNotEnded() {
        Preconditions.checkState(!ended, "Already ended");
    }

    private void doRead(final Buffer writeBuff, final int offset, final ByteBuffer buff, final long startPosition, long currentPosition, final Handler<AsyncResult<Result>> handler) {
        try {
            ch.read(buff, currentPosition, null, new java.nio.channels.CompletionHandler<Integer, Object>() {

                long currentPos = currentPosition;

                private void done(boolean eof) {
                    context.runOnContext((v) -> {
                        buff.flip();
                        writeBuff.setBytes(offset, buff);
                        buff.compact();
                        handler.handle(Future.succeededFuture(new Result(writeBuff, eof, (int) (currentPos - startPosition))));
                    });
                }

                public void completed(Integer bytesRead, Object attachment) {
                    if (bytesRead == -1) {
                        //End of file
                        done(true);
                    } else if (buff.hasRemaining()) {
                        // partial read
                        currentPos += bytesRead;
                        // resubmit
                        doRead(writeBuff, offset, buff, startPosition, currentPos, handler);
                    } else {
                        // It's been fully written
                        currentPos += bytesRead;
                        done(false);
                    }
                }

                public void failed(Throwable t, Object attachment) {
                    context.runOnContext((v) -> {
                        handler.handle(Future.failedFuture(t));
                    });
                }
            });
        } catch (Throwable e) {
            context.runOnContext(event -> {
                handler.handle(Future.failedFuture(e));
            });
        }
    }

    private static class Result {

        private boolean eof;
        private final Buffer buffer;
        private int bytesRead;

        public Result(Buffer buffer, boolean eof, int bytesRead) {
            this.buffer = buffer;
            this.eof = eof;
            this.bytesRead = bytesRead;
        }
    }
}
