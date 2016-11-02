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

package org.sfs.integration.java.test.blob;

import com.google.common.base.Optional;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.Server;
import org.sfs.TestSubscriber;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.HeaderBlob;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.nodes.Nodes;
import org.sfs.nodes.RemoteNode;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collections;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Optional.of;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.Files.hash;
import static java.lang.System.out;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.Files.size;
import static java.nio.file.Files.write;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.util.MessageDigestFactory.MD5;
import static org.sfs.util.MessageDigestFactory.SHA512;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertTrue;
import static rx.Observable.just;

public class RemoteBlobActionsTest extends BaseTestVerticle {


    @Test
    public void testLargeFile(TestContext context) throws IOException {

        final byte[] data = new byte[256];
        getCurrentInstance().nextBytesBlocking(data);
        int dataSize = 256 * 1024 * 1024;

        final Path tempFile = createTempFile(tmpDir, "", "");

        int bytesWritten = 0;
        try (OutputStream out = newOutputStream(tempFile)) {
            while (bytesWritten < dataSize) {
                out.write(data);
                bytesWritten += data.length;
            }
        }

        Async async = context.async();
        aVoid()
                .map(aVoid -> {
                    Nodes nodes = vertxContext().verticle().nodes();
                    RemoteNode remoteNode =
                            new RemoteNode(
                                    vertxContext(),
                                    nodes.getResponseTimeout(),
                                    Collections.singletonList(nodes.getHostAndPort()));
                    return remoteNode;
                })
                .flatMap(new Func1<RemoteNode, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(final RemoteNode remoteNode) {
                        out.println("ZZZZ1");
                        return just((Void) null)
                                .flatMap(new PutData(VERTX_CONTEXT, context, remoteNode, tempFile))
                                .map(headerBlob -> {
                                    out.println("Done writing");
                                    return headerBlob;
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob blob) {
                                        out.println("ZZZZ2");
                                        return remoteNode.acknowledge(blob.getVolume(), blob.getPosition())
                                                .map(Optional::get);
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob headerBlob) {
                                        out.println("ZZZZ3");
                                        return remoteNode.createReadStream(headerBlob.getVolume(), headerBlob.getPosition(), absent(), absent())
                                                .map(Optional::get)
                                                .flatMap(new Func1<ReadStreamBlob, Observable<HeaderBlob>>() {
                                                    @Override
                                                    public Observable<HeaderBlob> call(final ReadStreamBlob readStreamBlob) {
                                                        final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512);
                                                        out.println("ZZZZ4");
                                                        return readStreamBlob.produce(digestWriteStream)
                                                                .map(aVoid -> {
                                                                    byte[] sha512;
                                                                    try {
                                                                        sha512 = hash(tempFile.toFile(), sha512()).asBytes();
                                                                    } catch (IOException e) {
                                                                        throw new RuntimeException(e);
                                                                    }
                                                                    assertArrayEquals(context, sha512, digestWriteStream.getDigest(SHA512).get());
                                                                    return readStreamBlob;
                                                                });
                                                    }
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob headerBlob) {
                                        out.println("ZZZZ5");
                                        return remoteNode.createReadStream(headerBlob.getVolume(), headerBlob.getPosition(), of(2L), of(1L))
                                                .map(Optional::get)
                                                .flatMap(new Func1<ReadStreamBlob, Observable<HeaderBlob>>() {
                                                    @Override
                                                    public Observable<HeaderBlob> call(final ReadStreamBlob readStreamBlob) {
                                                        final BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                                                        return readStreamBlob.produce(bufferWriteStream)
                                                                .map(aVoid -> {
                                                                    byte[] buffer = bufferWriteStream.toBuffer().getBytes();
                                                                    assertEquals(context, 1, buffer.length);
                                                                    assertEquals(context, data[2], buffer[0]);
                                                                    return readStreamBlob;
                                                                });
                                                    }
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ6");
                                        return remoteNode.checksum(blob.getVolume(), blob.getPosition(), absent(), absent())
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob blob) {
                                        out.println("ZZZZ7");
                                        return remoteNode.delete(blob.getVolume(), blob.getPosition())
                                                .map(Optional::get);
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ8");
                                        return remoteNode.acknowledge(blob.getVolume(), blob.getPosition())
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, !headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ9");
                                        return remoteNode.delete(blob.getVolume(), blob.getPosition())
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ20");
                                        return remoteNode.createReadStream(blob.getVolume(), blob.getPosition(), absent(), absent())
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, !headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .map(new ToVoid<>());
                    }
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void test(TestContext context) throws IOException {

        final byte[] data = new byte[256];
        getCurrentInstance().nextBytesBlocking(data);

        final Path tempFile1 = createTempFile(tmpDir, "", "");
        write(tempFile1, data);


        Async async = context.async();
        aVoid()
                .map(aVoid -> {
                    Nodes nodes = vertxContext().verticle().nodes();
                    RemoteNode remoteNode =
                            new RemoteNode(
                                    vertxContext(),
                                    nodes.getResponseTimeout(),
                                    Collections.singletonList(nodes.getHostAndPort()));
                    return remoteNode;
                })
                .flatMap(new Func1<RemoteNode, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(final RemoteNode remoteNode) {
                        out.println("A0");
                        return just((Void) null)
                                .flatMap(new PutData(VERTX_CONTEXT, context, remoteNode, tempFile1))
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob blob) {
                                        out.println("A1");
                                        return remoteNode.acknowledge(blob.getVolume(), blob.getPosition())
                                                .map(Optional::get);
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob headerBlob) {
                                        out.println("A2");
                                        return remoteNode.createReadStream(headerBlob.getVolume(), headerBlob.getPosition(), absent(), absent())
                                                .map(Optional::get)
                                                .flatMap(readStreamBlob -> {
                                                    out.println("A3");
                                                    final BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                                                    return readStreamBlob.produce(bufferWriteStream)
                                                            .map(aVoid -> {
                                                                assertArrayEquals(context, data, bufferWriteStream.toBuffer().getBytes());
                                                                return readStreamBlob;
                                                            });
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob headerBlob) {
                                        out.println("A4");
                                        return remoteNode.createReadStream(headerBlob.getVolume(), headerBlob.getPosition(), of(2L), of(1L))
                                                .map(Optional::get)
                                                .flatMap(new Func1<ReadStreamBlob, Observable<HeaderBlob>>() {
                                                    @Override
                                                    public Observable<HeaderBlob> call(final ReadStreamBlob readStreamBlob) {
                                                        out.println("A5");
                                                        final BufferWriteEndableWriteStream bufferWriteStream = new BufferWriteEndableWriteStream();
                                                        return readStreamBlob.produce(bufferWriteStream)
                                                                .map(aVoid -> {
                                                                    byte[] buffer = bufferWriteStream.toBuffer().getBytes();
                                                                    assertEquals(context, 1, buffer.length);
                                                                    assertEquals(context, data[2], buffer[0]);
                                                                    return readStreamBlob;
                                                                });
                                                    }
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ5");
                                        return remoteNode.checksum(blob.getVolume(), blob.getPosition(), absent(), absent(), SHA512)
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(HeaderBlob blob) {
                                        out.println("ZZZZ4");
                                        return remoteNode.delete(blob.getVolume(), blob.getPosition())
                                                .map(Optional::get);
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ3");
                                        return remoteNode.acknowledge(blob.getVolume(), blob.getPosition())
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, !headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ2");
                                        return remoteNode.delete(blob.getVolume(), blob.getPosition())
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .flatMap(new Func1<HeaderBlob, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(final HeaderBlob blob) {
                                        out.println("ZZZZ1");
                                        return remoteNode.createReadStream(blob.getVolume(), blob.getPosition(), absent(), absent())
                                                .map(headerBlobOptional -> {
                                                    assertTrue(context, !headerBlobOptional.isPresent());
                                                    return blob;
                                                });
                                    }
                                })
                                .map(new ToVoid<>());
                    }
                })
                .count()
                .map(new ToVoid<>())
                .map(new Func1<Void, Void>() {
                    @Override
                    public Void call(Void aVoid) {
                        out.println("Done!");
                        return null;
                    }
                })
                .subscribe(new TestSubscriber(context, async));

    }

    protected static class PutData implements Func1<Void, Observable<HeaderBlob>> {

        private final TestContext testContext;
        private final VertxContext<Server> vertx;
        private final RemoteNode remoteNode;
        private final Path data;

        public PutData(VertxContext<Server> vertxContext, TestContext testContext, RemoteNode remoteNode, Path data) {
            this.testContext = testContext;
            this.remoteNode = remoteNode;
            this.data = data;
            this.vertx = vertxContext;
        }

        @Override
        public Observable<HeaderBlob> call(Void aVoid) {
            return aVoid()
                    .flatMap(aVoid1 -> {
                        ObservableFuture<AsyncFile> handler = RxHelper.observableFuture();
                        OpenOptions openOptions = new OpenOptions();
                        openOptions.setCreate(true)
                                .setRead(true)
                                .setWrite(true);
                        vertx.vertx().fileSystem().open(data.toString(), openOptions, handler.toHandler());
                        return handler;
                    }).flatMap(asyncFile -> {
                        final long size;
                        final byte[] md5;
                        final byte[] sha512;
                        try {
                            size = size(data);
                            md5 = hash(data.toFile(), md5()).asBytes();
                            sha512 = hash(data.toFile(), sha512()).asBytes();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        return just(fromNullable(getFirst(vertx.verticle().nodes().volumeManager().volumes(), null)))
                                .map(volumeOptional -> volumeOptional.get())
                                .flatMap(new Func1<String, Observable<HeaderBlob>>() {
                                    @Override
                                    public Observable<HeaderBlob> call(String volumeId) {
                                        out.println("PPPP1");
                                        return remoteNode.createWriteStream(volumeId, size, SHA512, MD5)
                                                .flatMap(nodeWriteStream -> {
                                                    out.println("PPPP2");
                                                    return nodeWriteStream.consume(asyncFile);
                                                })
                                                .map(blob -> {
                                                    out.println("PPPP3");
                                                    asyncFile.close();
                                                    return blob;
                                                })
                                                .map(blob -> {
                                                    assertArrayEquals(testContext, md5, blob.getDigest(MD5).get());
                                                    assertArrayEquals(testContext, sha512, blob.getDigest(SHA512).get());
                                                    return blob;
                                                });
                                    }
                                });
                    });
        }
    }
}
