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

package org.sfs.encryption;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.CipherEndableWriteStream;
import org.sfs.io.CipherReadStream;
import org.sfs.io.EndableReadStream;

public abstract class Algorithm {

    public abstract long maxEncryptInputSize();

    public abstract long encryptOutputSize(long size);

    public abstract byte[] getSalt();

    public abstract byte[] encrypt(byte[] buffer);

    public abstract byte[] decrypt(byte[] buffer);

    public abstract Buffer encrypt(Buffer buffer);

    public abstract Buffer decrypt(Buffer buffer);

    public abstract <T extends CipherEndableWriteStream> T encrypt(BufferEndableWriteStream writeStream);

    public abstract <T extends CipherEndableWriteStream> T decrypt(BufferEndableWriteStream writeStream);

    public abstract <T extends CipherReadStream> T encrypt(EndableReadStream<Buffer> readStream);

    public abstract <T extends CipherReadStream> T decrypt(EndableReadStream<Buffer> readStream);
}
