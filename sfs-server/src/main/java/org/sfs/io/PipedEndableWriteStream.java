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

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;

public class PipedEndableWriteStream implements BufferEndableWriteStream {

    private static final Logger LOGGER = getLogger(PipedEndableWriteStream.class);
    private int maxWrites = 128 * 1024;
    private int lwm = maxWrites / 2;
    private boolean ended = false;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> endHandler;
    private Handler<Void> drainHandler;
    private PipedReadStream readStream;
    private Queue<Buffer> chunks = new ArrayDeque<>();
    private int writesOutstanding;

    public PipedEndableWriteStream(PipedReadStream readStream) {
        this.readStream = readStream;
        this.readStream.connect(this);
    }

    public PipedReadStream readStream() {
        return readStream;
    }

    @Override
    public PipedEndableWriteStream drainHandler(Handler<Void> handler) {
        this.drainHandler = handler;
        handleDrained();
        return this;
    }

    @Override
    public PipedEndableWriteStream write(Buffer data) {
        checkState(!ended, "Already Ended");
        write0(data);
        handleDrained();
        return this;
    }

    private void write0(Buffer data) {
        if (chunks.isEmpty()) {
            if (!readStream.write(data)) {
                writesOutstanding += data.length();
                chunks.offer(data);
            }
        } else {
            writesOutstanding += data.length();
            chunks.offer(data);
            readStream.drainWriteStream();
        }
    }

    @Override
    public PipedEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public PipedEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        this.maxWrites = maxSize;
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return writesOutstanding >= maxWrites;
    }

    public boolean writeQueueEmpty() {
        return writesOutstanding <= 0;
    }

    public boolean writeQueueDrained() {
        return writesOutstanding <= lwm;
    }

    public boolean ended() {
        return ended;
    }

    @Override
    public PipedEndableWriteStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        readStream.drainWriteStream();
        handleEnd();
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        checkState(!ended, "Already Ended");
        ended = true;
        write0(buffer);
        readStream.drainWriteStream();
        handleEnd();
    }

    @Override
    public void end() {
        checkState(!ended, "Already Ended");
        ended = true;
        readStream.drainWriteStream();
        handleEnd();
    }

    protected Buffer poll() {
        Buffer chunk = chunks.poll();
        if (chunk != null) {
            writesOutstanding -= chunk.length();
        }
        handleDrained();
        handleEnd();
        return chunk;
    }

    protected void handleEnd() {
        if (ended && writeQueueEmpty()) {
            Handler<Void> handler = endHandler;
            if (handler != null && writeQueueEmpty()) {
                endHandler = null;
                handler.handle(null);
            }
        }
    }

    protected void handleDrained() {
        if (!ended && writeQueueDrained()) {
            Handler<Void> handler = drainHandler;
            if (handler != null && writeQueueDrained()) {
                drainHandler = null;
                handler.handle(null);
            }
        }
    }
}
