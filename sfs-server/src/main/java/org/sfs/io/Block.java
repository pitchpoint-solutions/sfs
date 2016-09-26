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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.GeneratedMessage;
import io.vertx.core.buffer.Buffer;

import java.util.Arrays;

public abstract class Block<A extends GeneratedMessage> {

    protected static int FRAME_HASH_SIZE = 16; // murmur32_128 hash
    protected static int FRAME_LENGTH_SIZE = 4; // size of the header frame

    protected static int FRAME_HASH_OFFSET = 0;
    protected static int FRAME_LENGTH_OFFSET = FRAME_HASH_OFFSET + FRAME_HASH_SIZE;
    protected static int FRAME_DATA_OFFSET = FRAME_LENGTH_OFFSET + FRAME_LENGTH_SIZE;

    protected static byte[] checksum(byte[] data) {
        return Hashing.murmur3_128().hashBytes(data).asBytes();
    }

    private final A value;
    private final int blockSize;
    private final int frameDataSize;

    protected Block(A value, int blockSize) {
        this.value = value;
        this.blockSize = blockSize;
        this.frameDataSize = blockSize - (FRAME_HASH_SIZE + FRAME_LENGTH_SIZE);
    }

    public A getValue() {
        return value;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public Frame<Buffer> toBuffer() {
        byte[] frame = getValue().toByteArray();
        Preconditions.checkState(frame.length <= frameDataSize, "Frame length was %s, expected %s", frame.length, frameDataSize);
        byte[] checksum = checksum(frame);

        Buffer buffer = Buffer.buffer();
        buffer.setBytes(FRAME_HASH_OFFSET, checksum);
        buffer.setInt(FRAME_LENGTH_OFFSET, frame.length);
        buffer.setBytes(FRAME_DATA_OFFSET, frame);

        Preconditions.checkState(buffer.length() <= blockSize, "Buffer size was %s, expected %s", buffer.length(), blockSize);

        byte[] remaining = new byte[blockSize - buffer.length()];
        if (remaining.length > 0) {
            buffer.setBytes(buffer.length(), remaining);
            Preconditions.checkState(buffer.length() <= blockSize, "Buffer size was %s, expected %s", buffer.length(), blockSize);
        }

        return new Frame<>(checksum, buffer);
    }

    public static Frame<Buffer> encodeFrame(Buffer value) {
        byte[] frame = value.getBytes();
        byte[] checksum = checksum(frame);

        Buffer buffer = Buffer.buffer();
        buffer.setBytes(FRAME_HASH_OFFSET, checksum);
        buffer.setInt(FRAME_LENGTH_OFFSET, frame.length);
        buffer.setBytes(FRAME_DATA_OFFSET, frame);

        return new Frame<>(checksum, buffer);
    }

    public static Optional<Frame<byte[]>> decodeFrame(Buffer buffer, boolean validateChecksum) {
        int length = buffer.length();

        final byte[] frame;
        final byte[] expectedChecksum;
        try {
            int frameSize = buffer.getInt(FRAME_LENGTH_OFFSET);
            Preconditions.checkArgument(frameSize >= 0 && frameSize < length, "Frame size was %s, expected 0 to %s", frameSize, length);
            frame = buffer.getBytes(FRAME_DATA_OFFSET, FRAME_DATA_OFFSET + frameSize);
            expectedChecksum = buffer.getBytes(FRAME_HASH_OFFSET, FRAME_HASH_OFFSET + FRAME_HASH_SIZE);
        } catch (Throwable e) {
            return Optional.absent();
        }

        Frame<byte[]> f = new Frame<byte[]>(expectedChecksum, frame) {

            @Override
            public boolean isChecksumValid() {
                return Arrays.equals(expectedChecksum, checksum(frame));
            }
        };

        if (validateChecksum) {
            if (!f.isChecksumValid()) {
                Preconditions.checkState(
                        false,
                        "Checksum was %s, expected %s",
                        BaseEncoding.base64().encode(checksum(frame)),
                        BaseEncoding.base64().encode(expectedChecksum));
            }
        }

        return Optional.of(f);
    }

    public static class Frame<A> {
        private final byte[] checksum;
        private final A data;

        public Frame(byte[] checksum, A data) {
            this.checksum = checksum;
            this.data = data;
        }

        public byte[] getChecksum() {
            return checksum;
        }

        public boolean isChecksumValid() {
            return true;
        }

        public A getData() {
            return data;
        }
    }
}
