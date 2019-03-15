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

public class ReplayReadStream implements EndableReadStream<Buffer> {

    private static final Logger log = getLogger(ReplayReadStream.class);
    private Buffer buffer;
    private Handler<Void> endHandler;
    private Handler<Buffer> dataHandler;
    private Handler<Throwable> exceptionHandler;
    private boolean paused = false;
    private long replayCount = 0;
    private final long replay;
    private boolean readInProgress = false;

    public ReplayReadStream(Buffer buffer, long replay) {
        this.buffer = buffer;
        this.replay = replay;
    }

    @Override
    public boolean isEnded() {
        return replayCount >= replay;
    }

    public int length() {
        return buffer.length();
    }

    @Override
    public ReplayReadStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    @Override
    public ReplayReadStream handler(Handler<Buffer> handler) {
        this.dataHandler = handler;
        if (!paused && dataHandler != null) {
            handleData(buffer);
        }
        return this;
    }

    @Override
    public ReplayReadStream pause() {
        this.paused = true;
        return this;
    }

    @Override
    public ReplayReadStream resume() {
        if (paused) {
            paused = false;
            if (dataHandler != null) {
                handleData(buffer);
            }
        }
        return this;
    }

    @Override
    public ReadStream<Buffer> fetch(long amount) {
        return this;
    }

    @Override
    public ReplayReadStream exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    private void handleEnd() {
        if (endHandler != null) {
            endHandler.handle(null);
        }
    }

    private void handleData(Buffer buffer) {
        if (dataHandler != null) {
            if (!readInProgress) {
                readInProgress = true;
                while (true) {
                    if (replayCount >= replay) {
                        handleEnd();
                        break;
                    } else {
                        dataHandler.handle(buffer);
                        replayCount++;
                        if (paused) {
                            break;
                        }
                    }
                }
                readInProgress = false;
            }
        }
    }

    private void handleException(Throwable t) {
        if (exceptionHandler != null && t instanceof Exception) {
            exceptionHandler.handle(t);
        } else {
            log.error("Unhandled exception", t);
        }
    }
}