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

package org.sfs.integration.java.test.farm;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.logging.Logger;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.sfs.TestSubscriber;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.integration.java.BaseTestVerticle;
import org.sfs.integration.java.func.WaitForCluster;
import org.sfs.io.DigestEndableWriteStream;
import org.sfs.io.NullEndableWriteStream;
import org.sfs.nodes.Nodes;
import org.sfs.nodes.VolumeReplicaGroup;
import org.sfs.nodes.XNode;
import org.sfs.rx.Holder2;
import org.sfs.rx.ToVoid;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

import static com.google.common.base.Optional.absent;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.hash.Hashing.sha512;
import static com.google.common.io.Files.hash;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.newOutputStream;
import static java.nio.file.Files.size;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.sfs.util.MessageDigestFactory.MD5;
import static org.sfs.util.MessageDigestFactory.SHA512;
import static org.sfs.util.PrngRandom.getCurrentInstance;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;
import static org.sfs.util.VertxAssert.assertTrue;
import static rx.Observable.from;
import static rx.Observable.just;

public class ReplicatedWriteTest extends BaseTestVerticle {

    private static final Logger LOGGER = getLogger(ReplicatedWriteTest.class);

    @Test
    public void testLarge(TestContext context) throws IOException {
        final byte[] data = new byte[256];
        getCurrentInstance().nextBytes(data);
        int dataSize = 256 * 1024;

        final Path tempFile1 = createTempFile(tmpDir, "", "");

        int bytesWritten = 0;
        try (OutputStream out = newOutputStream(tempFile1, WRITE, SYNC)) {
            while (bytesWritten < dataSize) {
                out.write(data);
                bytesWritten += data.length;
            }
        }

        final long size = size(tempFile1);

        final byte[] md5 = hash(tempFile1.toFile(), md5()).asBytes();
        final byte[] sha512 = hash(tempFile1.toFile(), sha512()).asBytes();

        OpenOptions openOptions = new OpenOptions();
        openOptions.setCreate(true)
                .setRead(true)
                .setWrite(true);

        final AsyncFile asyncFile = VERTX.fileSystem().openBlocking(tempFile1.toString(), openOptions);

        Nodes nodes = vertxContext().verticle().nodes();

        Async async = context.async();
        just((Void) null)
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().getNodeStats().forceUpdate(VERTX_CONTEXT))
                .flatMap(aVoid -> VERTX_CONTEXT.verticle().getClusterInfo().forceRefresh(VERTX_CONTEXT))
                .flatMap(new WaitForCluster(VERTX, HTTP_CLIENT))
                .flatMap(aVoid -> {
                    final VolumeManager volumeManager = nodes.volumeManager();
                    return volumeManager.newVolume(VERTX_CONTEXT)
                            .map(new ToVoid<>())
                            .flatMap(aVoid1 -> VERTX_CONTEXT.verticle().getNodeStats().forceUpdate(VERTX_CONTEXT))
                            .flatMap(aVoid1 -> VERTX_CONTEXT.verticle().getClusterInfo().forceRefresh(VERTX_CONTEXT))
                            .flatMap(aVoid1 -> {
                                assertEquals(context, 2, Iterables.size(volumeManager.volumes()));
                                return just(new VolumeReplicaGroup(vertxContext(), 2)
                                        .setAllowSameNode(true));
                            });
                })
                .flatMap(volumeReplicaGroup ->
                        just((Void) null)
                                .flatMap(aVoid -> volumeReplicaGroup.consume(size, newArrayList(MD5, SHA512), asyncFile))
                                .map(responses -> {
                                    DigestBlob blob;
                                    for (Holder2<XNode, DigestBlob> h : responses) {
                                        blob = h.value1();
                                        assertArrayEquals(context, md5, blob.getDigest(MD5).get());
                                        assertArrayEquals(context, sha512, blob.getDigest(SHA512).get());
                                    }
                                    return responses;

                                })
                                .flatMap(responses ->
                                        from(responses)
                                                .flatMap(response -> {
                                                    DigestBlob digestBlob = response.value1();
                                                    XNode xNode = response.value0();
                                                    LOGGER.debug("Doing ack for volume " + digestBlob.getVolume() + " position " + digestBlob.getPosition());
                                                    return xNode.acknowledge(digestBlob.getVolume(), digestBlob.getPosition())
                                                            .map(headerBlobOptional -> {
                                                                assertTrue(context, headerBlobOptional.isPresent());
                                                                return (Void) null;
                                                            });
                                                })
                                                .count()
                                                .map(integer -> responses)
                                )
                                .flatMap(responses ->
                                        from(responses)
                                                .flatMap(response -> {
                                                    DigestBlob digestBlob = response.value1();
                                                    XNode xNode = response.value0();
                                                    return xNode.createReadStream(digestBlob.getVolume(), digestBlob.getPosition(), absent(), absent())
                                                            .map(Optional::get)
                                                            .flatMap(readStreamBlob -> {
                                                                final DigestEndableWriteStream digestWriteStream = new DigestEndableWriteStream(new NullEndableWriteStream(), SHA512);
                                                                return readStreamBlob.produce(digestWriteStream)
                                                                        .map(aVoid -> {
                                                                            assertArrayEquals(context, sha512, digestWriteStream.getDigest(SHA512).get());
                                                                            return (Void) null;
                                                                        });
                                                            });

                                                })
                                                .count()
                                                .map(integer -> responses)
                                )
                                .flatMap(responses ->
                                        from(responses)
                                                .flatMap(response -> {
                                                    DigestBlob digestBlob = response.value1();
                                                    XNode xNode = response.value0();
                                                    return xNode.delete(digestBlob.getVolume(), digestBlob.getPosition())
                                                            .map(headerBlobOptional -> {
                                                                assertTrue(context, headerBlobOptional.isPresent());
                                                                return (Void) null;
                                                            });
                                                })
                                                .count()
                                                .map(integer -> responses)
                                )
                )
                .map(new ToVoid<>())
                .subscribe(new TestSubscriber(context, async));

    }

}
