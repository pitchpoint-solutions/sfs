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

import io.vertx.core.buffer.Buffer;

import java.io.IOException;
import java.io.OutputStream;

public class BufferOutputStream extends OutputStream {

    private Buffer buffer;

    public BufferOutputStream() {
        buffer = Buffer.buffer();
    }

    public Buffer toBuffer() {
        return buffer;
    }

    public void reset() {
        buffer = Buffer.buffer();
    }

    @Override
    public void write(byte[] b) throws IOException {
        buffer.appendBytes(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        buffer.appendBytes(b, off, len);
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void write(int b) throws IOException {
        buffer.appendByte((byte) b);
    }
}
