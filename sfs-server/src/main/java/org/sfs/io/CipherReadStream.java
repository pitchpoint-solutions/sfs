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

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.bouncycastle.crypto.io.CipherOutputStream;
import org.bouncycastle.crypto.modes.AEADBlockCipher;

import javax.crypto.Cipher;
import java.io.IOException;
import java.io.OutputStream;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class CipherReadStream implements EndableReadStream<Buffer> {

    private static final Logger LOGGER = getLogger(CipherReadStream.class);
    private ReadStream<Buffer> delegate;
    private Handler<Void> delegateEndHandler;
    private Handler<Throwable> delegateExceptionHandler;
    private Handler<Buffer> delegateDataHandler;
    private OutputStream cipherOutputStream;
    private ReadStreamDataHandlerOutputStream readStreamDataHandlerOutputStream;
    private boolean ended = false;
    private Handler<Void> endHandler = new Handler<Void>() {
        @Override
        public void handle(Void event) {
            Handler<Void> handler = delegateEndHandler;
            delegateDataHandler = null;
            if (handler != null) {
                handler.handle(null);
            }
        }
    };
    private Handler<Buffer> dataHandler = new Handler<Buffer>() {
        @Override
        public void handle(Buffer buffer) {
            if (delegateDataHandler != null) {
                delegateDataHandler.handle(buffer);
            }
        }
    };


    private Handler<Void> outputStreamEndHandler = new Handler<Void>() {
        @Override
        public void handle(Void event) {
            try {
                cipherOutputStream.close();
                ended = true;
                endHandler.handle(null);
            } catch (IOException e) {
                handleError(e);
            }
        }
    };
    private Handler<Buffer> outputStreamDataHandler = new Handler<Buffer>() {
        @Override
        public void handle(Buffer buffer) {
            try {
                byte[] data = buffer.getBytes();
                cipherOutputStream.write(data);
            } catch (IOException e) {
                handleError(e);
            }
        }
    };

    @Override
    public boolean isEnded() {
        return ended;
    }

    @Override
    public ReadStream<Buffer> fetch(long amount) {
        return this;
    }

    public CipherReadStream(ReadStream<Buffer> delegate, AEADBlockCipher aeadBlockCipher) {
        this.delegate = delegate;
        this.readStreamDataHandlerOutputStream = new ReadStreamDataHandlerOutputStream(dataHandler);
        this.cipherOutputStream = new CipherOutputStream(readStreamDataHandlerOutputStream, aeadBlockCipher);
    }

    public CipherReadStream(ReadStream<Buffer> delegate, Cipher cipher) {
        this.delegate = delegate;
        this.readStreamDataHandlerOutputStream = new ReadStreamDataHandlerOutputStream(dataHandler);
        this.cipherOutputStream = new javax.crypto.CipherOutputStream(readStreamDataHandlerOutputStream, cipher);
    }


    @Override
    public CipherReadStream endHandler(Handler<Void> endHandler) {
        this.delegateEndHandler = endHandler;
        if (endHandler != null) {
            this.delegate.endHandler(this.outputStreamEndHandler);
        } else {
            this.delegate.endHandler(null);
        }
        return this;
    }

    @Override
    public CipherReadStream handler(Handler<Buffer> handler) {
        delegateDataHandler = handler;
        if (handler == null) {
            delegate.handler(null);
        } else {
            delegate.handler(outputStreamDataHandler);
        }
        return this;
    }

    @Override
    public CipherReadStream pause() {
        delegate.pause();
        return this;
    }

    @Override
    public CipherReadStream resume() {
        delegate.resume();
        return this;
    }

    @Override
    public CipherReadStream exceptionHandler(Handler<Throwable> handler) {
        this.delegateExceptionHandler = handler;
        delegate.exceptionHandler(handler);
        return this;
    }

    protected void handleError(Throwable e) {
        if (delegateExceptionHandler != null) {
            delegateExceptionHandler.handle(e);
        } else {
            LOGGER.warn("Unhandled Exception", e);
        }
    }
}
