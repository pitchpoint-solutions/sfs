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
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class CountingReadStream implements ReadStream<Buffer> {

    private static final Logger LOGGER = getLogger(CountingReadStream.class);
    private final ReadStream<Buffer> delegate;
    private long count = 0;
    private Handler<Buffer> delegateDataHandler;
    private Handler<Buffer> dataHandler = new Handler<Buffer>() {
        @Override
        public void handle(Buffer event) {
            count += event.length();
            if (delegateDataHandler != null) {
                delegateDataHandler.handle(event);
            }
        }
    };

    public CountingReadStream(ReadStream<Buffer> delegate) {
        this.delegate = delegate;
    }

    public long count() {
        return count;
    }

    @Override
    public CountingReadStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public CountingReadStream handler(Handler<Buffer> handler) {
        if (handler == null) {
            this.delegateDataHandler = null;
            delegate.handler(null);
        } else {
            this.delegateDataHandler = handler;
            this.delegate.handler(dataHandler);
        }
        return this;
    }

    @Override
    public CountingReadStream pause() {
        delegate.pause();
        return this;
    }

    @Override
    public CountingReadStream resume() {
        delegate.resume();
        return this;
    }

    @Override
    public CountingReadStream exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }
}
