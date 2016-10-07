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

import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import org.bouncycastle.crypto.BufferedBlockCipher;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.StreamCipher;
import org.bouncycastle.crypto.engines.AESFastEngine;
import org.bouncycastle.crypto.io.InvalidCipherTextIOException;
import org.bouncycastle.crypto.modes.AEADBlockCipher;
import org.bouncycastle.crypto.modes.GCMBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class CipherWriteStreamValidation {

    public static final int KEY_SIZE_BYTES = 32;
    public static final int NONCE_SIZE_BYTES = 12;
    private static final int MAC_SIZE_BITS = 96;
    private byte[] salt;
    private final GCMBlockCipher encryptor;
    private final GCMBlockCipher decryptor;

    public CipherWriteStreamValidation(byte[] secretBytes, byte[] salt) {
        this.salt = salt.clone();
        secretBytes = secretBytes.clone();
        if (secretBytes.length != KEY_SIZE_BYTES) {
            secretBytes = Hashing.sha256().hashBytes(secretBytes).asBytes();
        }
        try {
            KeyParameter key = new KeyParameter(secretBytes);
            AEADParameters params = new AEADParameters(key, MAC_SIZE_BITS, this.salt);

            this.encryptor = new GCMBlockCipher(new AESFastEngine());
            this.encryptor.init(true, params);

            this.decryptor = new GCMBlockCipher(new AESFastEngine());
            this.decryptor.init(false, params);

        } catch (Exception e) {
            throw new RuntimeException("could not create cipher for AES256", e);
        } finally {
            Arrays.fill(secretBytes, (byte) 0);
        }
    }


    public byte[] getSalt() {
        return salt.clone();
    }


    public byte[] decrypt(byte[] data) {
        if (data == null) {
            return null;
        }
        try (InputStream inputStream = decrypt(new ByteArrayInputStream(data))) {
            return ByteStreams.toByteArray(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] encrypt(byte[] data) {
        if (data == null) {
            return null;
        }
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (OutputStream outputStream = encrypt(byteArrayOutputStream)) {
                outputStream.write(data);
            }
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public InputStream decrypt(InputStream inputStream) {
        return new CipherInputStream(inputStream, decryptor);
    }


    public OutputStream encrypt(OutputStream outputStream) {
        return new CipherOutputStream(outputStream, encryptor);
    }

    /**
     * A CipherInputStream is composed of an InputStream and a cipher so that read() methods return data
     * that are read in from the underlying InputStream but have been additionally processed by the
     * Cipher. The cipher must be fully initialized before being used by a CipherInputStream.
     * <p/>
     * For example, if the Cipher is initialized for decryption, the
     * CipherInputStream will attempt to read in data and decrypt them,
     * before returning the decrypted data.
     */
    private static class CipherInputStream
            extends FilterInputStream {
        private BufferedBlockCipher bufferedBlockCipher;
        private StreamCipher streamCipher;
        private AEADBlockCipher aeadBlockCipher;

        private final byte[] buf;
        private final byte[] inBuf;

        private int bufOff;
        private int maxBuf;
        private boolean finalized;

        private static final int INPUT_BUF_SIZE = 2048;

        /**
         * Constructs a CipherInputStream from an InputStream and a
         * BufferedBlockCipher.
         */
        public CipherInputStream(
                InputStream is,
                BufferedBlockCipher cipher) {
            super(is);

            this.bufferedBlockCipher = cipher;

            int outSize = cipher.getOutputSize(INPUT_BUF_SIZE);

            buf = new byte[(outSize > INPUT_BUF_SIZE) ? outSize : INPUT_BUF_SIZE];
            inBuf = new byte[INPUT_BUF_SIZE];
        }

        public CipherInputStream(
                InputStream is,
                StreamCipher cipher) {
            super(is);

            this.streamCipher = cipher;

            buf = new byte[INPUT_BUF_SIZE];
            inBuf = new byte[INPUT_BUF_SIZE];
        }

        /**
         * Constructs a CipherInputStream from an InputStream and an AEADBlockCipher.
         */
        public CipherInputStream(InputStream is, AEADBlockCipher cipher) {
            super(is);

            this.aeadBlockCipher = cipher;

            int outSize = cipher.getOutputSize(INPUT_BUF_SIZE);

            buf = new byte[(outSize > INPUT_BUF_SIZE) ? outSize : INPUT_BUF_SIZE];
            inBuf = new byte[INPUT_BUF_SIZE];
        }

        /**
         * Read data from underlying stream and process with cipher until end of stream or some data is
         * available after cipher processing.
         *
         * @return -1 to indicate end of stream, or the number of bytes (> 0) available.
         */
        private int nextChunk()
                throws IOException {
            if (finalized) {
                return -1;
            }

            bufOff = 0;
            maxBuf = 0;

            // Keep reading until EOF or cipher processing produces data
            while (maxBuf == 0) {
                int read = in.read(inBuf);
                if (read == -1) {
                    finaliseCipher();
                    if (maxBuf == 0) {
                        return -1;
                    }
                    return maxBuf;
                }

                try {
                    if (bufferedBlockCipher != null) {
                        maxBuf = bufferedBlockCipher.processBytes(inBuf, 0, read, buf, 0);
                    } else if (aeadBlockCipher != null) {
                        maxBuf = aeadBlockCipher.processBytes(inBuf, 0, read, buf, 0);
                    } else {
                        streamCipher.processBytes(inBuf, 0, read, buf, 0);
                        maxBuf = read;
                    }
                } catch (Exception e) {
                    throw new IOException("Error processing stream " + e);
                }
            }
            return maxBuf;
        }

        private void finaliseCipher()
                throws IOException {
            try {
                finalized = true;
                if (bufferedBlockCipher != null) {
                    maxBuf = bufferedBlockCipher.doFinal(buf, 0);
                } else if (aeadBlockCipher != null) {
                    maxBuf = aeadBlockCipher.doFinal(buf, 0);
                } else {
                    maxBuf = 0; // a stream cipher
                }
            } catch (final InvalidCipherTextException e) {
                throw new InvalidCipherTextIOException("Error finalising cipher", e);
            } catch (Exception e) {
                throw new IOException("Error finalising cipher " + e);
            }
        }

        /**
         * Reads data from the underlying stream and processes it with the cipher until the cipher
         * outputs data, and returns the next available byte.
         * <p/>
         * If the underlying stream is exhausted by this call, the cipher will be finalised.
         *
         * @throws java.io.IOException                                     if there was an error closing the input stream.
         * @throws org.bouncycastle.crypto.io.InvalidCipherTextIOException if the data read from the stream was invalid ciphertext
         *                                                                 (e.g. the cipher is an AEAD cipher and the ciphertext tag check fails).
         */
        public int read()
                throws IOException {
            if (bufOff >= maxBuf) {
                if (nextChunk() < 0) {
                    return -1;
                }
            }

            return buf[bufOff++] & 0xff;
        }

        /**
         * Reads data from the underlying stream and processes it with the cipher until the cipher
         * outputs data, and then returns up to <code>b.length</code> bytes in the provided array.
         * <p/>
         * If the underlying stream is exhausted by this call, the cipher will be finalised.
         *
         * @param b the buffer into which the data is read.
         * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no
         * more data because the end of the stream has been reached.
         * @throws java.io.IOException                                     if there was an error closing the input stream.
         * @throws org.bouncycastle.crypto.io.InvalidCipherTextIOException if the data read from the stream was invalid ciphertext
         *                                                                 (e.g. the cipher is an AEAD cipher and the ciphertext tag check fails).
         */
        public int read(
                byte[] b)
                throws IOException {
            return read(b, 0, b.length);
        }

        /**
         * Reads data from the underlying stream and processes it with the cipher until the cipher
         * outputs data, and then returns up to <code>len</code> bytes in the provided array.
         * <p/>
         * If the underlying stream is exhausted by this call, the cipher will be finalised.
         *
         * @param b   the buffer into which the data is read.
         * @param off the set offset in the destination array <code>b</code>
         * @param len the maximum number of bytes read.
         * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no
         * more data because the end of the stream has been reached.
         * @throws java.io.IOException                                     if there was an error closing the input stream.
         * @throws org.bouncycastle.crypto.io.InvalidCipherTextIOException if the data read from the stream was invalid ciphertext
         *                                                                 (e.g. the cipher is an AEAD cipher and the ciphertext tag check fails).
         */
        public int read(
                byte[] b,
                int off,
                int len)
                throws IOException {
            if (bufOff >= maxBuf) {
                if (nextChunk() < 0) {
                    return -1;
                }
            }

            int toSupply = Math.min(len, available());
            System.arraycopy(buf, bufOff, b, off, toSupply);
            bufOff += toSupply;
            return toSupply;
        }

        public long skip(
                long n)
                throws IOException {
            if (n <= 0) {
                return 0;
            }

            int skip = (int) Math.min(n, available());
            bufOff += skip;
            return skip;
        }

        public int available()
                throws IOException {
            return maxBuf - bufOff;
        }

        /**
         * Closes the underlying input stream and finalises the processing of the data by the cipher.
         *
         * @throws java.io.IOException                                     if there was an error closing the input stream.
         * @throws org.bouncycastle.crypto.io.InvalidCipherTextIOException if the data read from the stream was invalid ciphertext
         *                                                                 (e.g. the cipher is an AEAD cipher and the ciphertext tag check fails).
         */
        public void close()
                throws IOException {
            try {
                in.close();
            } finally {
                if (!finalized) {
                    // Reset the cipher, discarding any data buffered in it
                    // Errors in cipher finalisation trump I/O error closing input
                    finaliseCipher();
                }
            }
            maxBuf = bufOff = 0;
        }

        public void mark(int readlimit) {
        }

        public void reset()
                throws IOException {
        }

        public boolean markSupported() {
            return false;
        }

    }

    private static class CipherOutputStream
            extends FilterOutputStream {
        private BufferedBlockCipher bufferedBlockCipher;
        private StreamCipher streamCipher;
        private AEADBlockCipher aeadBlockCipher;

        private final byte[] oneByte = new byte[1];
        private byte[] buf;

        /**
         * Constructs a CipherOutputStream from an OutputStream and a
         * BufferedBlockCipher.
         */
        public CipherOutputStream(
                OutputStream os,
                BufferedBlockCipher cipher) {
            super(os);
            this.bufferedBlockCipher = cipher;
        }

        /**
         * Constructs a CipherOutputStream from an OutputStream and a
         * BufferedBlockCipher.
         */
        public CipherOutputStream(
                OutputStream os,
                StreamCipher cipher) {
            super(os);
            this.streamCipher = cipher;
        }

        /**
         * Constructs a CipherOutputStream from an OutputStream and a AEADBlockCipher.
         */
        public CipherOutputStream(OutputStream os, AEADBlockCipher cipher) {
            super(os);
            this.aeadBlockCipher = cipher;
        }

        /**
         * Writes the specified byte to this output stream.
         *
         * @param b the <code>byte</code>.
         * @throws java.io.IOException if an I/O error occurs.
         */
        public void write(
                int b)
                throws IOException {
            oneByte[0] = (byte) b;

            if (streamCipher != null) {
                out.write(streamCipher.returnByte((byte) b));
            } else {
                write(oneByte, 0, 1);
            }
        }

        /**
         * Writes <code>b.length</code> bytes from the specified byte array
         * to this output stream.
         * <p/>
         * The <code>write</code> method of
         * <code>CipherOutputStream</code> calls the <code>write</code>
         * method of three arguments with the three arguments
         * <code>b</code>, <code>0</code>, and <code>b.length</code>.
         *
         * @param b the data.
         * @throws java.io.IOException if an I/O error occurs.
         * @see #write(byte[], int, int)
         */
        public void write(
                byte[] b)
                throws IOException {
            write(b, 0, b.length);
        }

        /**
         * Writes <code>len</code> bytes from the specified byte array
         * starting at offset <code>off</code> to this output stream.
         *
         * @param b   the data.
         * @param off the set offset in the data.
         * @param len the number of bytes to write.
         * @throws java.io.IOException if an I/O error occurs.
         */
        public void write(
                byte[] b,
                int off,
                int len)
                throws IOException {
            ensureCapacity(len);

            if (bufferedBlockCipher != null) {
                int outLen = bufferedBlockCipher.processBytes(b, off, len, buf, 0);

                if (outLen != 0) {
                    out.write(buf, 0, outLen);
                }
            } else if (aeadBlockCipher != null) {
                int outLen = aeadBlockCipher.processBytes(b, off, len, buf, 0);

                if (outLen != 0) {
                    out.write(buf, 0, outLen);
                }
            } else {
                streamCipher.processBytes(b, off, len, buf, 0);

                out.write(buf, 0, len);
            }
        }

        /**
         * Ensure the ciphertext buffer has space sufficient to accept an upcoming output.
         *
         * @param outputSize the size of the pending update.
         */
        private void ensureCapacity(int outputSize) {
            // This overestimates buffer on updates for AEAD/padded, but keeps it simple.
            int bufLen;
            if (bufferedBlockCipher != null) {
                bufLen = bufferedBlockCipher.getOutputSize(outputSize);
            } else if (aeadBlockCipher != null) {
                bufLen = aeadBlockCipher.getOutputSize(outputSize);
            } else {
                bufLen = outputSize;
            }
            if ((buf == null) || (buf.length < bufLen)) {
                buf = new byte[bufLen];
            }
        }

        /**
         * Flushes this output stream by forcing any buffered output bytes
         * that have already been processed by the encapsulated cipher object
         * to be written out.
         * <p/>
         * <p/>
         * Any bytes buffered by the encapsulated cipher
         * and waiting to be processed by it will not be written out. For example,
         * if the encapsulated cipher is a block cipher, and the total number of
         * bytes written using one of the <code>write</code> methods is less than
         * the cipher's block size, no bytes will be written out.
         *
         * @throws java.io.IOException if an I/O error occurs.
         */
        public void flush()
                throws IOException {
            out.flush();
        }

        /**
         * Closes this output stream and releases any system resources
         * associated with this stream.
         * <p/>
         * This method invokes the <code>doFinal</code> method of the encapsulated
         * cipher object, which causes any bytes buffered by the encapsulated
         * cipher to be processed. The result is written out by calling the
         * <code>flush</code> method of this output stream.
         * <p/>
         * This method resets the encapsulated cipher object to its initial state
         * and calls the <code>close</code> method of the underlying output
         * stream.
         *
         * @throws java.io.IOException                                     if an I/O error occurs.
         * @throws org.bouncycastle.crypto.io.InvalidCipherTextIOException if the data written to this stream was invalid ciphertext
         *                                                                 (e.g. the cipher is an AEAD cipher and the ciphertext tag check fails).
         */
        public void close()
                throws IOException {
            ensureCapacity(0);
            IOException error = null;
            try {
                if (bufferedBlockCipher != null) {
                    int outLen = bufferedBlockCipher.doFinal(buf, 0);

                    if (outLen != 0) {
                        out.write(buf, 0, outLen);
                    }
                } else if (aeadBlockCipher != null) {
                    int outLen = aeadBlockCipher.doFinal(buf, 0);

                    if (outLen != 0) {
                        out.write(buf, 0, outLen);
                    }
                }
            } catch (final InvalidCipherTextException e) {
                error = new InvalidCipherTextIOException("Error finalising cipher data", e);
            } catch (Exception e) {
                error = new IOException("Error closing stream: " + e);
            }

            try {
                flush();
                out.close();
            } catch (IOException e) {
                // Invalid ciphertext takes precedence over close error
                if (error == null) {
                    error = e;
                }
            }
            if (error != null) {
                throw error;
            }
        }
    }
}