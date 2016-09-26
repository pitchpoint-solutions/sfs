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

package org.sfs.io;


import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

public class BufferWriteEndableWriteStream implements BufferEndableWriteStream {

    private Handler<Throwable> exceptionHandler;
    private Handler<Void> endHandler;
    private Handler<Void> drainHandler;
    private Buffer buffer;
    private boolean ended = false;

    public BufferWriteEndableWriteStream(Buffer buffer) {
        this.buffer = buffer;
    }

    public BufferWriteEndableWriteStream() {
        this(Buffer.buffer());
    }

    public Buffer toBuffer() {
        return buffer;
    }

    public Handler<Void> endHandler() {
        return endHandler;
    }

    public Handler<Void> drainHandler() {
        return drainHandler;
    }

    public Handler<Throwable> exceptionHandler() {
        return exceptionHandler;
    }

    @Override
    public BufferWriteEndableWriteStream write(Buffer data) {
        buffer.appendBuffer(data);
        handleDrain();
        return this;
    }

    @Override
    public BufferWriteEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public BufferWriteEndableWriteStream drainHandler(Handler<Void> handler) {
        this.drainHandler = handler;
        handleDrain();
        return this;
    }

    @Override
    public BufferWriteEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public BufferWriteEndableWriteStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public void end(Buffer data) {
        ended = true;
        buffer.appendBuffer(data);
        handleDrain();
        handleEnd();
    }

    @Override
    public void end() {
        ended = true;
        handleDrain();
        handleEnd();
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

    protected void handleDrain() {
        Handler<Void> handler = drainHandler;
        if (handler != null) {
            drainHandler = null;
            handler.handle(null);
        }
    }
}
