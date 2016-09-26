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
import com.google.common.collect.FluentIterable;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.sfs.rx.Holder2;
import org.sfs.util.MessageDigestFactory;

import java.security.MessageDigest;
import java.util.EnumMap;
import java.util.Map;

public class DigestReadStream implements ReadStream<Buffer> {

    private final ReadStream<Buffer> delegate;
    private final Map<MessageDigestFactory, byte[]> cache = new EnumMap<>(MessageDigestFactory.class);
    private final Map<MessageDigestFactory, MessageDigest> digests = new EnumMap<>(MessageDigestFactory.class);
    // keep an array referencing MessageDigests so it's fast to iterate when hashing Buffers
    private final MessageDigest[] messageDigests;
    private Handler<Buffer> delegateDataHandler;
    private Handler<Buffer> dataHandler = new Handler<Buffer>() {
        @Override
        public void handle(Buffer buffer) {
            byte[] bytes = buffer.getBytes();
            for (MessageDigest md : messageDigests) {
                md.update(bytes);
            }
            if (delegateDataHandler != null) {
                delegateDataHandler.handle(buffer);
            }
        }
    };

    public DigestReadStream(ReadStream<Buffer> delegate, MessageDigestFactory messageDigest, MessageDigestFactory... mdfs) {
        this.delegate = delegate;
        this.messageDigests = new MessageDigest[1 + mdfs.length];
        MessageDigest md = messageDigest.instance();
        this.messageDigests[0] = md;
        digests.put(messageDigest, md);
        for (int i = 0; i < mdfs.length; i++) {
            MessageDigestFactory mdf = mdfs[i];
            md = mdf.instance();
            this.messageDigests[i + 1] = md;
            digests.put(mdf, md);
        }
    }

    public DigestReadStream(ReadStream<Buffer> delegate, MessageDigestFactory[] mdfs) {
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
    public DigestReadStream handler(Handler<Buffer> handler) {
        if (handler != null) {
            delegateDataHandler = handler;
            delegate.handler(dataHandler);
        } else {
            delegateDataHandler = null;
            delegate.handler(null);
        }
        return this;
    }

    @Override
    public DigestReadStream pause() {
        delegate.pause();
        return this;
    }

    @Override
    public DigestReadStream resume() {
        delegate.resume();
        return this;
    }

    @Override
    public DigestReadStream exceptionHandler(Handler<Throwable> handler) {
        delegate.exceptionHandler(handler);
        return this;
    }

    @Override
    public DigestReadStream endHandler(Handler<Void> endHandler) {
        delegate.endHandler(endHandler);
        return this;
    }
}
