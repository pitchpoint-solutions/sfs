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
import io.vertx.core.streams.ReadStream;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class PipedReadStream implements ReadStream<Buffer> {

    private static final Logger LOGGER = getLogger(PipedReadStream.class);
    private Handler<Void> endHandler;
    private Handler<Buffer> dataHandler;
    private boolean paused = false;
    private Handler<Throwable> exceptionHandler;
    private PipedEndableWriteStream writeStream;
    private boolean draining = false;

    protected void connect(PipedEndableWriteStream writeStream) {
        this.writeStream = writeStream;
    }

    public PipedReadStream() {
    }

    public boolean write(Buffer chunk) {
        if (!paused && writeStream.writeQueueEmpty()) {
            return write0(chunk);
        } else {
            return false;
        }
    }

    private boolean write0(Buffer chunk) {
        if (dataHandler != null) {
            dataHandler.handle(chunk);
            return true;
        }
        return false;
    }

    @Override
    public PipedReadStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public PipedReadStream handler(Handler<Buffer> handler) {
        this.dataHandler = handler;
        drainWriteStream();
        return this;
    }

    @Override
    public PipedReadStream pause() {
        paused = true;
        return this;
    }

    @Override
    public PipedReadStream resume() {
        paused = false;
        drainWriteStream();
        return this;
    }

    protected void drainWriteStream() {
        if (!draining) {
            try {
                while (true) {
                    Handler<Buffer> handler = dataHandler;
                    if (paused || handler == null) {
                        break;
                    }
                    Buffer chunk = writeStream.poll();
                    if (chunk != null) {
                        handler.handle(chunk);
                    } else {
                        break;
                    }
                }
            } finally {
                draining = false;
            }
            handleEnd();
        }
    }

    @Override
    public PipedReadStream exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    protected void handleEnd() {
        if (writeStream.ended() && writeStream.writeQueueEmpty()) {
            Handler<Void> handler = endHandler;
            if (handler != null) {
                endHandler = null;
                handler.handle(null);
            }
        }
    }
}
