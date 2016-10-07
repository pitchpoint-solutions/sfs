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
import io.vertx.core.streams.ReadStream;

public class BufferReadStream implements ReadStream<Buffer> {

    private final Buffer buffer;
    private boolean consumed = false;
    private Handler<Buffer> dataHandler;
    private boolean paused = false;
    private Handler<Void> endHandler;

    public BufferReadStream(Buffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public BufferReadStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public BufferReadStream handler(Handler<Buffer> handler) {
        this.dataHandler = handler;
        handleData();
        handleEnd();
        return this;
    }

    @Override
    public BufferReadStream pause() {
        paused = true;
        dataHandler = null;
        return this;
    }

    @Override
    public BufferReadStream resume() {
        paused = false;
        handleData();
        handleEnd();
        return this;
    }

    @Override
    public BufferReadStream exceptionHandler(Handler<Throwable> handler) {
        return this;
    }

    private void handleData() {
        if (!paused && !consumed && dataHandler != null) {
            Handler<Buffer> handler = dataHandler;
            dataHandler = null;
            consumed = true;
            handler.handle(buffer);
        }
    }

    private void handleEnd() {
        if (consumed && endHandler != null) {
            Handler<Void> handler = endHandler;
            endHandler = null;
            handler.handle(null);
        }
    }
}
