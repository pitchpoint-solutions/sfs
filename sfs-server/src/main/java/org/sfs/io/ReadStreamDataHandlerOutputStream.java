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

import java.io.IOException;
import java.io.OutputStream;

import static io.vertx.core.buffer.Buffer.buffer;

public class ReadStreamDataHandlerOutputStream extends OutputStream {

    private Handler<Buffer> dataHandler;

    public ReadStreamDataHandlerOutputStream(Handler<Buffer> dataHandler) {
        this.dataHandler = dataHandler;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void write(byte[] b) throws IOException {
        dataHandler.handle(buffer(b));
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        Buffer buffer = buffer(len);
        dataHandler.handle(buffer.appendBytes(b, off, len));
    }

    @Override
    public void write(int b) throws IOException {
        dataHandler.handle(buffer(1).appendByte((byte) b));
    }
}
