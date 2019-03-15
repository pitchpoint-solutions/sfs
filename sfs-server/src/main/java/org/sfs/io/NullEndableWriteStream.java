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

public class NullEndableWriteStream implements BufferEndableWriteStream {

    private boolean dirty = false;
    private boolean ended = false;
    private Handler<Void> drainHandler;
    private Handler<Void> endHandler;

    public NullEndableWriteStream() {
    }

    @Override
    public boolean isEnded() {
        return ended;
    }

    @Override
    public NullEndableWriteStream write(Buffer data) {
        dirty = true;
        handleDrain();
        return this;
    }

    @Override
    public NullEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        return this;
    }

    @Override
    public NullEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public NullEndableWriteStream drainHandler(Handler<Void> handler) {
        this.drainHandler = handler;
        handleDrain();
        return this;
    }

    @Override
    public NullEndableWriteStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public void end(Buffer buffer) {
        ended = true;
        handleEnd();
    }

    @Override
    public void end() {
        ended = true;
        handleEnd();
    }

    protected void handleDrain() {
        if (dirty) {
            dirty = false;
            Handler<Void> handler = drainHandler;
            if (handler != null) {
                drainHandler = null;
                handler.handle(null);
            }
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
}
