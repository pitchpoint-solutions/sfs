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


import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import org.sfs.rx.Holder2;
import org.sfs.util.MessageDigestFactory;

import java.security.MessageDigest;
import java.util.EnumMap;
import java.util.Map;

public class DigestEndableWriteStream implements BufferEndableWriteStream {

    private final BufferEndableWriteStream delegate;
    private final Map<MessageDigestFactory, byte[]> cache = new EnumMap<>(MessageDigestFactory.class);
    private final Map<MessageDigestFactory, MessageDigest> digests = new EnumMap<>(MessageDigestFactory.class);
    // keep an array referencing MessageDigests so it's fast to iterate when hashing Buffers
    private final MessageDigest[] messageDigests;

    public DigestEndableWriteStream(BufferEndableWriteStream delegate, MessageDigestFactory... mdfs) {
        this.delegate = delegate;
        this.messageDigests = new MessageDigest[mdfs.length];
        for (int i = 0; i < mdfs.length; i++) {
            MessageDigestFactory mdf = mdfs[i];
            MessageDigest md = mdf.instance();
            this.messageDigests[i] = md;
            digests.put(mdf, md);
        }
    }

    public Optional<byte[]> getDigest(MessageDigestFactory key) {
        byte[] hash = cache.get(key);
        if (hash == null) {
            hash = digests.get(key).digest();
            cache.put(key, hash);
            return Optional.fromNullable(hash);
        }
        return Optional.of(hash);
    }

    public Iterable<Holder2<MessageDigestFactory, byte[]>> digests() {
        return FluentIterable.from(digests.keySet())
                .filter(input -> getDigest(input).isPresent())
                .transform(input -> {
                    Optional<byte[]> hash = getDigest(input);
                    return new Holder2<>(input, hash.get());
                });
    }

    @Override
    public DigestEndableWriteStream write(Buffer data) {
        byte[] bytes = data.getBytes();
        for (MessageDigest md : messageDigests) {
            md.update(bytes);
        }
        delegate.write(data);
        return this;
    }

    @Override
    public DigestEndableWriteStream setWriteQueueMaxSize(int maxSize) {
        delegate.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return delegate.writeQueueFull();
    }

    @Override
    public DigestEndableWriteStream drainHandler(Handler<Void> handler) {
        delegate.drainHandler(handler);
        return this;
    }

    @Override
    public DigestEndableWriteStream exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public DigestEndableWriteStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }

    @Override
    public void end(Buffer data) {
        byte[] bytes = data.getBytes();
        for (MessageDigest md : messageDigests) {
            md.update(bytes);
        }
        delegate.end(data);
    }

    @Override
    public void end() {
        delegate.end();
    }

    public static class Sha512DigestEndableWriteStream extends DigestEndableWriteStream {

        public Sha512DigestEndableWriteStream(BufferEndableWriteStream delegate) {
            super(delegate, MessageDigestFactory.SHA512);
        }

        public byte[] getMessageDigest() {
            return getDigest(MessageDigestFactory.SHA512).get();
        }
    }

    public static class Md5DigestEndableWriteStream extends DigestEndableWriteStream {

        public Md5DigestEndableWriteStream(BufferEndableWriteStream delegate) {
            super(delegate, MessageDigestFactory.MD5);
        }

        public byte[] getMessageDigest() {
            return getDigest(MessageDigestFactory.MD5).get();
        }
    }
}
