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

package org.sfs.filesystem.containerdump;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.elasticsearch.search.SearchHit;
import org.sfs.Server;
import org.sfs.SfsVertx;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.encryption.Algorithm;
import org.sfs.encryption.AlgorithmDef;
import org.sfs.filesystem.JournalFile;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.DeflateEndableWriteStream;
import org.sfs.io.EndableWriteStream;
import org.sfs.io.FileBackedBuffer;
import org.sfs.io.MultiEndableWriteStream;
import org.sfs.io.PipedEndableWriteStream;
import org.sfs.io.PipedReadStream;
import org.sfs.nodes.compute.object.CopySegmentsReadStreams;
import org.sfs.nodes.compute.object.ReadSegments;
import org.sfs.rx.Defer;
import org.sfs.rx.NullSubscriber;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.util.ExceptionHelper;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import rx.Observable;
import rx.Subscriber;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.protobuf.ByteString.copyFrom;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.singletonList;
import static org.sfs.encryption.AlgorithmDef.getPreferred;
import static org.sfs.protobuf.XVolume.XDumpFile.CompressionType.DEFLATE;
import static org.sfs.protobuf.XVolume.XDumpFile.CompressionType.NONE;
import static org.sfs.protobuf.XVolume.XDumpFile.FirstHeader.Builder;
import static org.sfs.protobuf.XVolume.XDumpFile.FirstHeader.newBuilder;
import static org.sfs.protobuf.XVolume.XDumpFile.Header;
import static org.sfs.protobuf.XVolume.XDumpFile.Header.Type.VERSION_01;
import static org.sfs.protobuf.XVolume.XDumpFile.Version01;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.rx.RxHelper.iterate;
import static org.sfs.vo.PersistentObject.fromSearchHit;
import static rx.Observable.using;

public class DumpFileWriter implements EndableWriteStream<SearchHit> {

    private static final Logger LOGGER = getLogger(DumpFileWriter.class);
    public static final String DUMP_FILE_NAME = "dump.object.001";
    private static final int FILE_THRESHOLD = 8192;
    private final VertxContext<Server> vertxContext;
    private final SfsVertx vertx;
    private final Elasticsearch elasticsearch;
    private final JournalFile dumpFile;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> drainHandler;
    private Handler<Void> endHandler;
    private boolean writeQueueFull = false;
    private boolean ended = false;
    private final PersistentContainer persistentContainer;
    private final String containerId;
    private byte[] key;
    private AlgorithmDef algorithmDef;
    private boolean encrypt = false;
    private boolean compress = false;
    private Path tempFileDirectory;
    private boolean wroteFirst = false;

    public DumpFileWriter(VertxContext<Server> vertxContext, PersistentContainer persistentContainer, JournalFile dumpFile) {
        this.vertxContext = vertxContext;
        this.elasticsearch = vertxContext.verticle().elasticsearch();
        this.dumpFile = dumpFile;
        this.vertx = vertxContext.vertx();
        this.persistentContainer = persistentContainer;
        this.containerId = persistentContainer.getId();
        this.tempFileDirectory = vertxContext.verticle().sfsFileSystem().tmpDirectory();
    }

    public DumpFileWriter enableDataEncryption(byte[] key) {
        this.key = key;
        this.algorithmDef = getPreferred();
        this.encrypt = true;
        return this;
    }

    public DumpFileWriter enableDataCompression() {
        this.compress = true;
        return this;
    }

    @Override
    public DumpFileWriter drainHandler(Handler<Void> handler) {
        checkNotEnded();
        drainHandler = handler;
        handleDrain();
        return this;
    }

    @Override
    public DumpFileWriter write(SearchHit data) {
        checkNotEnded();
        writeQueueFull = true;
        aVoid()
                .flatMap(aVoid -> writeFirst())
                .flatMap(aVoid -> write0(data))
                .subscribe(new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        writeQueueFull = false;
                        handleDrain();
                    }

                    @Override
                    public void onError(Throwable e) {
                        writeQueueFull = false;
                        handleError(e);
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
        return this;
    }

    @Override
    public DumpFileWriter exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public DumpFileWriter setWriteQueueMaxSize(int maxSize) {
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return writeQueueFull;
    }

    @Override
    public DumpFileWriter endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        handleEnd();
        return this;
    }

    @Override
    public void end(SearchHit data) {
        checkNotEnded();
        ended = true;
        writeQueueFull = true;
        aVoid()
                .flatMap(aVoid -> writeFirst())
                .flatMap(aVoid -> write0(data))
                .subscribe(new Subscriber<Void>() {
                    @Override
                    public void onCompleted() {
                        writeQueueFull = false;
                        handleDrain();
                        handleEnd();
                    }

                    @Override
                    public void onError(Throwable e) {
                        writeQueueFull = false;
                        handleError(e);
                    }

                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }

    @Override
    public void end() {
        checkNotEnded();
        ended = true;
        handleDrain();
        handleEnd();
    }

    protected void handleError(Throwable e) {
        if (exceptionHandler != null) {
            exceptionHandler.handle(e);
        } else {
            LOGGER.error("Unhandled Exception", e);
        }

    }

    private void handleDrain() {
        if (!writeQueueFull) {
            Handler<Void> handler = drainHandler;
            if (handler != null) {
                drainHandler = null;
                handler.handle(null);
            }
        }
    }

    private void handleEnd() {
        if (ended) {
            Handler<Void> handler = endHandler;
            if (handler != null) {
                endHandler = null;
                handler.handle(null);
            }
        }
    }

    private void checkNotEnded() {
        checkState(!ended, "Already ended");
    }

    protected Observable<Void> writeFirst() {
        if (!wroteFirst) {
            wroteFirst = true;
            Builder firstHeaderBuilder =
                    newBuilder();
            firstHeaderBuilder.setEncrypted(encrypt);
            if (algorithmDef != null) {
                firstHeaderBuilder.setCipherName(algorithmDef.getAlgorithmName());
            }
            return dumpFile.append(vertx, buffer(firstHeaderBuilder.build().toByteArray()));
        } else {
            return aVoid();
        }
    }

    protected Observable<Void> write0(SearchHit data) {
        String index = data.index();

        checkState(elasticsearch.isObjectIndex(index), "%s it not an object index", index);

        PersistentObject persistentObject = fromSearchHit(persistentContainer, data);
        String objectId = persistentObject.getId();

        checkState(objectId.startsWith(containerId), "ObjectId %s does not start with ContainerId %s", objectId, containerId);

        return iterate(vertx, persistentObject.getVersions(), transientVersion -> {

            Header.Builder headerBuilder = Header.newBuilder();
            headerBuilder.setType(VERSION_01);
            headerBuilder.setMetadataCompressionType(NONE);
            headerBuilder.setDataCompressionType(NONE);

            Version01 exportObject = transientVersion.toExportObject();
            byte[] marshaledExportObject = exportObject.toByteArray();
            if (compress) {
                byte[] compressedMarshaledExportObject = deflate(marshaledExportObject);
                if (compressedMarshaledExportObject.length < marshaledExportObject.length) {
                    marshaledExportObject = compressedMarshaledExportObject;
                    headerBuilder.setMetadataCompressionType(DEFLATE);
                }
            }

            if (encrypt) {
                byte[] cipherMetadataSalt = algorithmDef.generateSaltBlocking();
                byte[] encryptedMarshaledExportObject = encrypt(cipherMetadataSalt, marshaledExportObject);
                headerBuilder.setCipherMetadataSalt(copyFrom(cipherMetadataSalt));
                marshaledExportObject = encryptedMarshaledExportObject;
            }

            headerBuilder.setData(copyFrom(marshaledExportObject));

            if (!transientVersion.isDeleted() && !transientVersion.getSegments().isEmpty()) {
                if (compress) {
                    // if compression is requested we have to first calculate how much space
                    // is needed in the journal so write to a memory/file buffered destination
                    // to calculate the size and then copy the memory/file buffered destination
                    // into the journal
                    FileBackedBuffer compressedFileBackedBuffer = new FileBackedBuffer(vertx, FILE_THRESHOLD, !encrypt, tempFileDirectory);
                    FileBackedBuffer uncompressedFileBackedBuffer = new FileBackedBuffer(vertx, FILE_THRESHOLD, !encrypt, tempFileDirectory);
                    return using(
                            () -> null,
                            aVoid -> {
                                BufferEndableWriteStream compressedDst = compressedFileBackedBuffer;
                                BufferEndableWriteStream uncompressedDst = uncompressedFileBackedBuffer;
                                if (encrypt) {
                                    byte[] cipherDataSalt = algorithmDef.generateSaltBlocking();
                                    compressedDst = encrypt(cipherDataSalt, compressedDst);
                                    uncompressedDst = encrypt(cipherDataSalt, uncompressedDst);
                                    headerBuilder.setCipherDataSalt(copyFrom(cipherDataSalt));
                                }
                                compressedDst = new DeflateEndableWriteStream(compressedDst);
                                BufferEndableWriteStream writeStream = new MultiEndableWriteStream(compressedDst, uncompressedDst);
                                return just(singletonList(transientVersion))
                                        .flatMap(new ReadSegments(vertxContext, writeStream, false))
                                        .map(new ToVoid<>())
                                        .flatMap(aVoid1 -> {
                                            ObservableFuture<Void> h = RxHelper.observableFuture();
                                            writeStream.endHandler(h::complete);
                                            writeStream.end();
                                            return h;
                                        })
                                        .flatMap(aVoid1 -> {
                                            if (compressedFileBackedBuffer.length() < uncompressedFileBackedBuffer.length()) {
                                                headerBuilder.setDataCompressionType(DEFLATE);
                                                byte[] marshaledMetadata = headerBuilder.build().toByteArray();
                                                ReadStream<Buffer> readStream = compressedFileBackedBuffer.readStream();
                                                return dumpFile.append(vertx, buffer(marshaledMetadata), compressedFileBackedBuffer.length(), readStream);
                                            } else {
                                                byte[] marshaledMetadata = headerBuilder.build().toByteArray();
                                                ReadStream<Buffer> readStream = uncompressedFileBackedBuffer.readStream();
                                                return dumpFile.append(vertx, buffer(marshaledMetadata), uncompressedFileBackedBuffer.length(), readStream);
                                            }
                                        })
                                        .map(new ToVoid<>())
                                        .onErrorResumeNext(throwable -> {
                                            if (ExceptionHelper.containsException(CopySegmentsReadStreams.SegmentReadStreamNotFoundException.class, throwable)) {
                                                return Defer.aVoid();
                                            } else {
                                                return Observable.error(throwable);
                                            }
                                        });
                            }
                            ,
                            aVoid ->
                                    combineSinglesDelayError(
                                            compressedFileBackedBuffer.close(),
                                            uncompressedFileBackedBuffer.close(),
                                            (aVoid1, aVoid2) -> (Void) null)
                                            .subscribe(new NullSubscriber<>()))
                            .map(aVoid -> TRUE);
                } else {

                    PipedReadStream pipedReadStream = new PipedReadStream();
                    BufferEndableWriteStream dst = new PipedEndableWriteStream(pipedReadStream);
                    long contentLength;
                    if (encrypt) {
                        byte[] cipherDataSalt = algorithmDef.generateSaltBlocking();
                        Algorithm algorithm = algorithmDef.create(cipherDataSalt, key);
                        contentLength = algorithm.encryptOutputSize(transientVersion.calculateLength().get());
                        dst = encrypt(cipherDataSalt, dst);
                        headerBuilder.setCipherDataSalt(copyFrom(cipherDataSalt));
                    } else {
                        contentLength = transientVersion.calculateLength().get();
                    }
                    byte[] marshaledMetadata = headerBuilder.build().toByteArray();
                    BufferEndableWriteStream writeStream = dst;
                    Observable<Void> oProducer =
                            just(singletonList(transientVersion))
                                    .flatMap(new ReadSegments(vertxContext, writeStream, false))
                                    .map(new ToVoid<>())
                                    .doOnNext(aVoid -> writeStream.end());
                    Observable<Void> oConsumer = dumpFile.append(vertx, buffer(marshaledMetadata), contentLength, pipedReadStream).map(new ToVoid<>());
                    return combineSinglesDelayError(oConsumer, oProducer, (aVoid11, aVoid12) -> TRUE)
                            .onErrorResumeNext(throwable -> {
                                if (ExceptionHelper.containsException(CopySegmentsReadStreams.SegmentReadStreamNotFoundException.class, throwable)) {
                                    // this will leave a black space in the file which needs to be handled during import
                                    return Defer.just(true);
                                } else {
                                    return Observable.error(throwable);
                                }
                            });

                }
            } else {
                byte[] marshaledMetadata = headerBuilder.build().toByteArray();
                return dumpFile.append(vertx, buffer(marshaledMetadata))
                        .map(aVoid -> TRUE);
            }
        }).map(new ToVoid<>());

    }

    private byte[] encrypt(byte[] salt, byte[] decrypted) {
        Algorithm algorithm = algorithmDef.create(key, salt);
        return algorithm.encrypt(decrypted);
    }

    private BufferEndableWriteStream encrypt(byte[] salt, BufferEndableWriteStream bufferStreamConsumer) {
        Algorithm algorithm = algorithmDef.create(key, salt);
        return algorithm.encrypt(bufferStreamConsumer);
    }

    private byte[] deflate(byte[] uncompressed) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)) {
            deflaterOutputStream.write(uncompressed);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private byte[] inflate(byte[] uncompressed) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (InflaterOutputStream deflaterOutputStream = new InflaterOutputStream(byteArrayOutputStream)) {
            deflaterOutputStream.write(uncompressed);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteArrayOutputStream.toByteArray();
    }
}
