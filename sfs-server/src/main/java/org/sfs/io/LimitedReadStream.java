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
import com.google.common.math.LongMath;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

public class LimitedReadStream implements EndableReadStream<Buffer> {

    private final EndableReadStream<Buffer> delegate;
    private final long length;
    private long bytesRead = 0;

    public LimitedReadStream(EndableReadStream<Buffer> delegate, long length) {
        this.delegate = delegate;
        this.length = length;
    }

    @Override
    public boolean isEnded() {
        return delegate.isEnded();
    }

    @Override
    public LimitedReadStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public LimitedReadStream handler(final Handler<Buffer> handler) {
        if (handler != null) {
            Handler<Buffer> wrapper = event -> {
                bytesRead = LongMath.checkedAdd(bytesRead, event.length());
                Preconditions.checkState(bytesRead <= length, "Read %s, max is %s", bytesRead, length);
                handler.handle(event);
            };
            delegate.handler(wrapper);
        } else {
            delegate.handler(null);
        }
        return this;
    }

    @Override
    public LimitedReadStream pause() {
        delegate.pause();
        return this;
    }

    @Override
    public LimitedReadStream resume() {
        delegate.resume();
        return this;
    }

    @Override
    public ReadStream<Buffer> fetch(long amount) {
        return this;
    }

    @Override
    public LimitedReadStream exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }
}
