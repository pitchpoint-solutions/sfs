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

package org.sfs.util;


import com.google.common.math.LongMath;
import io.vertx.core.buffer.Buffer;
import org.sfs.filesystem.Positional;

import java.util.Collections;
import java.util.Iterator;

public class Buffers {

    public static Iterable<Buffer> partition(final Buffer input, final int size) {
        int length = input.length();
        if (length <= size) {
            return Collections.singletonList(input);
        } else {
            return () -> new SliceIterator(input, size);
        }
    }

    public static Iterable<Positional<Buffer>> partition(final Positional<Buffer> input, final int size) {
        return () -> new Iterator<Positional<Buffer>>() {

            final long position = input.getPosition();
            final Buffer src = input.getValue();
            private long offset = position;
            private Iterator<Buffer> delegate = Buffers.partition(src, size).iterator();

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Positional<Buffer> next() {
                Buffer buffer = delegate.next();
                Positional<Buffer> mapped = new Positional<>(offset, buffer);
                offset = LongMath.checkedAdd(offset, size);
                return mapped;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

    }

    private static class SliceIterator implements Iterator<Buffer> {


        private final int size;
        private final Buffer source;
        private int position = 0;
        private int length;

        public SliceIterator(Buffer source, int size) {
            this.size = size;
            this.source = source;
            this.length = source.length();
        }

        @Override
        public boolean hasNext() {
            return position < length;
        }

        @Override
        public Buffer next() {
            Buffer slice = source.getBuffer(position, Math.min(position + size, length));
            position += size;
            return slice;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
