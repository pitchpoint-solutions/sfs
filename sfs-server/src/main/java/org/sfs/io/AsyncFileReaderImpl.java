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


import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;

public class AsyncFileReaderImpl implements AsyncFileReader {

    private final Logger log;

    private final AsynchronousFileChannel ch;
    private final Context context;
    private Handler<Throwable> exceptionHandler;

    private boolean paused;
    private Handler<Buffer> dataHandler;
    private Handler<Void> endHandler;
    private long readPos;
    private boolean readInProgress;
    private final int bufferSize;
    private final long startPosition;
    private long bytesRemaining;

    public AsyncFileReaderImpl(final SfsVertx vertx, long startPosition, int bufferSize, long length, AsynchronousFileChannel dataFile, Logger log) {
        this.log = log;
        this.bufferSize = bufferSize;
        this.readPos = startPosition;
        this.bytesRemaining = length;
        this.startPosition = startPosition;
        this.ch = dataFile;
        this.context = vertx.getOrCreateContext();
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

    private AsyncFileReaderImpl read(Buffer buffer, int offset, long position, int length, Handler<AsyncResult<Buffer>> handler) {
        try {
            ByteBuffer bb = ByteBuffer.allocate(length);
            doRead(buffer, offset, bb, position, handler);
            return this;
        } catch (Throwable e) {
            handleException(e);
            return this;
        }
    }

    private void doRead() {
        try {
            if (!readInProgress) {
                readInProgress = true;
                int countOfBytesToRead = (int) Math.min(bytesRemaining, bufferSize);
                final Buffer buff = Buffer.buffer(countOfBytesToRead);

                Handler<AsyncResult<Buffer>> handler = ar -> {
                    readInProgress = false;
                    try {
                        if (ar.succeeded()) {

                            Buffer buffer = ar.result();
                            int bufferLength = buffer.length();
                            bytesRemaining -= bufferLength;

                            if (bufferLength == 0) {
                                // Empty buffer represents end of file
                                handleEnd();
                            } else {
                                readPos += bufferLength;
                                handleData(buffer);
                                if (!paused && dataHandler != null) {
                                    doRead();
                                }
                            }
                        } else {
                            handleException(ar.cause());
                        }
                    } catch (Throwable e) {
                        handleException(e);
                    }
                };
                read(buff, 0, readPos, countOfBytesToRead, handler);
            }
        } catch (Throwable e) {
            handleException(e);
        }
    }

    @Override
    public AsyncFileReaderImpl handler(Handler<Buffer> handler) {
        this.dataHandler = handler;
        doRead();
        return this;
    }

    @Override
    public AsyncFileReaderImpl endHandler(Handler<Void> handler) {
        this.endHandler = handler;
        return this;
    }

    @Override
    public AsyncFileReaderImpl pause() {
        paused = true;
        return this;
    }

    @Override
    public AsyncFileReaderImpl resume() {
        paused = false;
        doRead();
        return this;
    }

    private void handleData(Buffer buffer) {
        if (dataHandler != null) {
            dataHandler.handle(buffer);
        }
    }

    private void handleEnd() {
        if (endHandler != null) {
            endHandler.handle(null);
        }
    }

    private void doRead(final Buffer writeBuff, final int offset, final ByteBuffer buff, final long position, final Handler<AsyncResult<Buffer>> handler) {

        ch.read(buff, position, null, new java.nio.channels.CompletionHandler<Integer, Object>() {

            long pos = position;

            private void done() {
                context.runOnContext(event -> {
                    buff.flip();
                    writeBuff.setBytes(offset, buff);
                    Future.succeededFuture(writeBuff).setHandler(handler);
                });
            }

            public void completed(Integer bytesRead, Object attachment) {
                if (bytesRead == -1) {
                    //End of file
                    done();
                } else if (buff.hasRemaining()) {
                    // partial read
                    pos += bytesRead;
                    // resubmit
                    doRead(writeBuff, offset, buff, pos, handler);
                } else {
                    // It's been fully written
                    done();
                }
            }

            public void failed(final Throwable t, Object attachment) {
                context.runOnContext(event -> Future.<Buffer>failedFuture(t).setHandler(handler));
            }
        });
    }
}
