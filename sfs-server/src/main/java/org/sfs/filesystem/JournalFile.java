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

package org.sfs.filesystem;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.streams.ReadStream;
import org.sfs.SfsVertx;
import org.sfs.io.Block;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.BufferWriteEndableWriteStream;
import org.sfs.rx.Defer;
import org.sfs.rx.ToVoid;
import rx.Observable;
import rx.exceptions.CompositeException;
import rx.functions.Func1;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.math.LongMath.checkedAdd;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.hash;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.sfs.filesystem.BlobFile.Status.STOPPED;
import static org.sfs.io.Block.decodeFrame;
import static org.sfs.io.Block.encodeFrame;
import static org.sfs.math.Rounding.up;
import static org.sfs.protobuf.XVolume.XJournal.Header;
import static org.sfs.protobuf.XVolume.XJournal.Header.Builder;
import static org.sfs.protobuf.XVolume.XJournal.Header.parseFrom;
import static org.sfs.protobuf.XVolume.XJournal.Super;
import static org.sfs.protobuf.XVolume.XJournal.Super.newBuilder;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.ExceptionHelper.containsException;
import static rx.Observable.error;

public class JournalFile {

    private static final Logger LOGGER = getLogger(JournalFile.class);
    private static final long NOT_SET = -1;
    private static final int SUPER_BLOCK_SIZE = 1024 * 1024;
    private static final long SUPER_BLOCK_POSITION_0 = 0;
    private static final long SUPER_BLOCK_POSITION_1 = 1024 * 1024;
    private static final long SUPER_BLOCK_RESERVED = SUPER_BLOCK_SIZE * 2;
    @VisibleForTesting
    protected static final int DEFAULT_BLOCK_SIZE = 75;
    private static final long DEFAULT_WRITE_STREAM_TIMEOUT = MINUTES.toMillis(1);
    private BlobFile blobFile;
    private int blockSize = (int) NOT_SET;
    private long logStartPosition = NOT_SET;
    private final AtomicReference<WritePosition> writePositionRef;
    private final Path path;

    public JournalFile(Path path, long writeStreamTimeout) {
        this.path = path;
        this.blobFile = new BlobFile(path, blockSize, writeStreamTimeout);
        this.writePositionRef = new AtomicReference<>(null);
    }

    public JournalFile(Path path) {
        this(path, DEFAULT_WRITE_STREAM_TIMEOUT);
    }

    public long firstLogEntryPosition() {
        return logStartPosition;
    }

    public Observable<Void> disableWrites(SfsVertx vertx) {
        return blobFile.disableWrites(vertx);
    }

    public Observable<Void> enableWrites(SfsVertx vertx) {
        return blobFile.enableWrites(vertx);
    }

    public Observable<Long> size(SfsVertx vertx) {
        return blobFile.size(vertx);
    }

    public Observable<Void> force(SfsVertx vertx, boolean metaData) {
        return blobFile.force(vertx, metaData);
    }

    public int getBlockSize() {
        return blockSize;
    }

    public Observable<Void> open(SfsVertx vertx) {
        return empty()
                .flatMap(aVoid -> {
                    // do some funcky stuff here to read the existing super block so that
                    // we have enough information to access the journal entries
                    BlobFile internalBlobFile = new BlobFile(path, SUPER_BLOCK_SIZE, DEFAULT_WRITE_STREAM_TIMEOUT);
                    return internalBlobFile.open(vertx, CREATE_NEW, READ, WRITE)
                            .flatMap(aVoid1 -> internalBlobFile.enableWrites(vertx))
                            .flatMap(aVoid1 -> {
                                Super superBlock =
                                        newBuilder()
                                                .setBlockSize(DEFAULT_BLOCK_SIZE)
                                                .build();
                                return setSuperBlock(vertx, internalBlobFile, superBlock)
                                        .map(aVoid11 -> superBlock);
                            })
                            .onErrorResumeNext(throwable -> {
                                if (containsException(FileAlreadyExistsException.class, throwable)) {
                                    return internalBlobFile.close(vertx)
                                            .flatMap(aVoid1 -> internalBlobFile.open(vertx, CREATE, READ, WRITE))
                                            .flatMap(aVoid1 -> internalBlobFile.enableWrites(vertx))
                                            .flatMap(aVoid1 -> getSuperBlock(vertx, internalBlobFile));
                                } else {
                                    return error(throwable);
                                }
                            })
                            .doOnNext(superBlock -> {
                                blockSize = superBlock.getBlockSize();
                                logStartPosition = up(SUPER_BLOCK_RESERVED, blockSize);
                            })
                            .map(new ToVoid<>())
                            .flatMap(aVoid1 -> internalBlobFile.disableWrites(vertx))
                            .flatMap(aVoid1 -> internalBlobFile.force(vertx, true))
                            .onErrorResumeNext(throwable -> {
                                if (!STOPPED.equals(blobFile.getStatus())) {
                                    return internalBlobFile.close(vertx)
                                            .onErrorResumeNext(throwable1 -> {
                                                return error(new CompositeException(throwable, throwable1));
                                            });
                                } else {
                                    return error(throwable);
                                }
                            })
                            .flatMap(aVoid1 -> internalBlobFile.close(vertx));
                })
                .flatMap(aVoid -> {
                    blobFile = new BlobFile(path, blockSize, DEFAULT_WRITE_STREAM_TIMEOUT);
                    return blobFile.open(vertx, CREATE, READ, WRITE);
                });
    }

    private Observable<Void> setSuperBlock(SfsVertx vertx, BlobFile internalBlobFile, Super superBlock) {
        Buffer buffer = buffer(superBlock.toByteArray());
        Block.Frame<Buffer> frame = encodeFrame(buffer);
        Buffer frameBuffer = frame.getData();
        int frameSize = frameBuffer.length();

        checkState(frameSize <= SUPER_BLOCK_SIZE, "Super block frame size was %s, which is greater block size of %s", frameSize, SUPER_BLOCK_SIZE);

        // write the super block twice so that we can recover from a failed
        // write
        return empty()
                .flatMap(aVoid -> internalBlobFile.consume(vertx, SUPER_BLOCK_POSITION_0, frameBuffer))
                .flatMap(aVoid -> internalBlobFile.force(vertx, true))
                .flatMap(aVoid -> internalBlobFile.consume(vertx, SUPER_BLOCK_POSITION_1, frameBuffer))
                .flatMap(aVoid -> internalBlobFile.force(vertx, true));
    }

    private Observable<Super> getSuperBlock(SfsVertx vertx, BlobFile internalBlobFile) {
        return empty()
                .flatMap(aVoid -> getSuperBlock0(vertx, internalBlobFile, SUPER_BLOCK_POSITION_0))
                .flatMap(superOptional -> {
                    if (superOptional.isPresent()) {
                        return just(superOptional);
                    } else {
                        return getSuperBlock0(vertx, internalBlobFile, SUPER_BLOCK_POSITION_1);
                    }
                })
                .doOnNext(superOptional -> checkState(superOptional.isPresent(), "Corrupt Super Block"))
                .map(Optional::get);
    }

    private Observable<Optional<Super>> getSuperBlock0(SfsVertx vertx, BlobFile internalBlobFile, long position) {
        return empty()
                .doOnNext(aVoid -> checkState(position == SUPER_BLOCK_POSITION_0 || position == SUPER_BLOCK_POSITION_1, "Position must be equal to %s or %s", SUPER_BLOCK_POSITION_0, SUPER_BLOCK_POSITION_1))
                .flatMap(aVoid -> {
                    BufferWriteEndableWriteStream bufferWriteStreamConsumer = new BufferWriteEndableWriteStream();
                    return internalBlobFile.produce(vertx, position, SUPER_BLOCK_SIZE, bufferWriteStreamConsumer)
                            .map(aVoid1 -> bufferWriteStreamConsumer.toBuffer());
                })
                .map(buffer -> {
                    Optional<Block.Frame<byte[]>> oFrame = decodeFrame(buffer, false);
                    if (oFrame.isPresent()) {
                        Block.Frame<byte[]> frame = oFrame.get();
                        return of(new ChecksummedPositional<byte[]>(position, frame.getData(), frame.getChecksum()) {
                            @Override
                            public boolean isChecksumValid() {
                                return frame.isChecksumValid();
                            }
                        });
                    } else {
                        return Optional.<ChecksummedPositional<byte[]>>absent();
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(this::parseSuperBlock)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(headerChecksummedPositional -> {
                    Super header = headerChecksummedPositional.getValue();
                    return header;
                })
                .map(Optional::of)
                .singleOrDefault(absent());
    }

    public Observable<Void> close(SfsVertx vertx) {
        return blobFile.close(vertx);
    }

    public Observable<Long> append(SfsVertx vertx, Buffer metadata, Buffer data) {
        int dataLength = data.length();
        return append0(vertx, metadata, dataLength)
                .flatMap(writePosition -> {
                    if (dataLength > 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Writing data @ position " + writePosition.getDataPosition());
                        }
                        return blobFile.consume(vertx, writePosition.getDataPosition(), data, false)
                                .doOnNext(aVoid -> {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("Done writing data @ position " + writePosition.getDataPosition());
                                    }
                                })
                                .map(aVoid -> writePosition.getHeaderPosition());
                    } else {
                        return Defer.just(writePosition.getHeaderPosition());
                    }
                });
    }

    public Observable<Long> append(SfsVertx vertx, Buffer metadata, long dataLength, ReadStream<Buffer> data) {
        return append0(vertx, metadata, dataLength)
                .flatMap(writePosition -> {
                    if (dataLength > 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Writing data @ position " + writePosition.getDataPosition());
                        }
                        return blobFile.consume(vertx, writePosition.getDataPosition(), dataLength, data, false)
                                .doOnNext(aVoid -> {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("Done writing data @ position " + writePosition.getDataPosition());
                                    }
                                })
                                .map(aVoid -> writePosition.getHeaderPosition());
                    } else {
                        return Defer.just(writePosition.getHeaderPosition());
                    }
                });
    }

    public Observable<Optional<Entry>> getFirstEntry(SfsVertx vertx) {
        return getEntry(vertx, firstLogEntryPosition());
    }

    public Observable<Optional<Entry>> getEntry(SfsVertx vertx, long position) {
        return empty()
                .doOnNext(aVoid -> blobFile.checkOpen())
                .doOnNext(aVoid -> checkLogEntryPosition(position))
                .flatMap(aVoid -> {
                    BufferWriteEndableWriteStream bufferWriteStreamConsumer = new BufferWriteEndableWriteStream();
                    return blobFile.produce(vertx, position, blockSize, bufferWriteStreamConsumer)
                            .map(aVoid1 -> bufferWriteStreamConsumer.toBuffer());
                })
                .map(buffer -> {
                    Optional<Block.Frame<byte[]>> oFrame = decodeFrame(buffer, false);
                    if (oFrame.isPresent()) {
                        Block.Frame<byte[]> frame = oFrame.get();
                        return of(new ChecksummedPositional<byte[]>(position, frame.getData(), frame.getChecksum()) {
                            @Override
                            public boolean isChecksumValid() {
                                return frame.isChecksumValid();
                            }
                        });
                    } else {
                        return Optional.<ChecksummedPositional<byte[]>>absent();
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(this::parseJournalHeader)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(headerChecksummedPositional -> {
                    Header header = headerChecksummedPositional.getValue();
                    return new Entry(blobFile, position, header);
                })
                .map(Optional::of)
                .singleOrDefault(absent());
    }

    public Observable<Void> scanFromFirst(SfsVertx vertx, Func1<Entry, Observable<Boolean>> func) {
        return empty()
                .flatMap(aVoid -> {
                    JournalScanner journalScanner = new JournalScanner(this, firstLogEntryPosition());
                    return journalScanner.scan(vertx, func);
                });
    }

    public Observable<Void> scan(SfsVertx vertx, long position, Func1<Entry, Observable<Boolean>> func) {
        return empty()
                .doOnNext(aVoid -> checkLogEntryPosition(position))
                .flatMap(aVoid -> {
                    JournalScanner journalScanner = new JournalScanner(this, position);
                    return journalScanner.scan(vertx, func);
                });
    }

    public Observable<Void> append(SfsVertx vertx, Buffer metadata) {
        return append0(vertx, metadata, 0)
                .map(new ToVoid<>());
    }

    protected Observable<WritePosition> append0(SfsVertx vertx, Buffer metadata, long dataLength) {
        long metadataLength = metadata.length();

        WritePosition writePosition = getAndIncWritePosition(metadataLength, dataLength);

        long previousHeaderPosition = writePosition.getPreviousHeaderPosition();
        long headerPosition = writePosition.getHeaderPosition();
        long metadataPosition = writePosition.getMetadataPosition();
        long dataPosition = writePosition.getDataPosition();
        long nextHeaderPosition = writePosition.getNextHeaderPosition();

        Builder headerBuilder =
                Header.newBuilder()
                        .setNextHeaderPosition(nextHeaderPosition)
                        .setMetaDataPosition(metadataPosition)
                        .setMetaDataLength(metadataLength)
                        .setDataPosition(dataPosition)
                        .setDataLength(dataLength);
        if (previousHeaderPosition >= 0) {
            headerBuilder = headerBuilder.setPreviousHeaderPositon(previousHeaderPosition);
        }
        Buffer headerBuffer = buffer(headerBuilder.build().toByteArray());
        Block.Frame<Buffer> headerFrame = encodeFrame(headerBuffer);
        Buffer headerFrameBuffer = headerFrame.getData();
        int headerFrameSize = headerFrameBuffer.length();

        checkState(headerFrameSize <= blockSize, "Header Frame size was %s, which is greater block size of %s", headerFrameSize, blockSize);

        return empty()
                .doOnNext(aVoid -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Writing header frame @ position " + headerPosition);
                    }
                })
                .flatMap(aVoid -> blobFile.consume(vertx, headerPosition, headerFrameBuffer, true))
                .doOnNext(aVoid -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Done Writing header frame @ position " + headerPosition);
                    }
                })
                .flatMap(aVoid -> {
                    if (metadataLength > 0) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Writing metadata @ position " + metadataPosition);
                        }
                        return blobFile.consume(vertx, metadataPosition, metadata, false)
                                .doOnNext(aVoid1 -> {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("Done writing metadata @ position " + metadataPosition);
                                    }
                                });
                    } else {
                        return empty();
                    }
                })
                .map(aVoid -> writePosition);
    }

    protected WritePosition getAndIncWritePosition(long metadataLength, long dataLength) {
        while (true) {
            WritePosition lastWritePosition = writePositionRef.get();
            if (lastWritePosition == null) {
                long headerPosition = firstLogEntryPosition();
                long metadataPosition = checkedAdd(headerPosition, blockSize);
                long dataPosition = checkedAdd(metadataPosition, metadataLength);
                long nextHeaderPosition = up(checkedAdd(dataPosition, dataLength), blockSize);
                WritePosition nextWritePosition = new WritePosition(NOT_SET, headerPosition, metadataPosition, dataPosition, nextHeaderPosition);
                assertWritePosition(nextWritePosition, metadataLength, dataLength);
                if (writePositionRef.compareAndSet(null, nextWritePosition)) {
                    return nextWritePosition;
                }
            } else {
                WritePosition nextWritePosition = nextWritePosition(lastWritePosition, metadataLength, dataLength);
                assertWritePosition(nextWritePosition, metadataLength, dataLength);
                if (writePositionRef.compareAndSet(lastWritePosition, nextWritePosition)) {
                    return nextWritePosition;
                }
            }
        }
    }

    private void checkLogEntryPosition(long position) {
        checkArgument(position != NOT_SET, "logStartPosition has not been set");
        checkArgument(position >= logStartPosition, "Position must be >= %s", logStartPosition);
    }

    private WritePosition nextWritePosition(WritePosition lastWritePosition, long metadataLength, long dataLength) {
        long previousHeaderPosition = lastWritePosition.getHeaderPosition();
        long headerPosition = lastWritePosition.getNextHeaderPosition();
        long metadataPosition = checkedAdd(headerPosition, blockSize);
        long dataPosition = checkedAdd(metadataPosition, metadataLength);
        long nextHeaderPosition = up(checkedAdd(dataPosition, dataLength), blockSize);
        return new WritePosition(previousHeaderPosition, headerPosition, metadataPosition, dataPosition, nextHeaderPosition);
    }

    protected void checkAligned(long value, int blockSize) {
        checkState(value % blockSize == 0, "%s is not multiple of %s", value, blockSize);
    }

    protected void assertWritePosition(WritePosition writePosition, long metadataLength, long dataLength) {

        checkState(blockSize != NOT_SET);

        long previousHeaderPosition = writePosition.getPreviousHeaderPosition();
        long headerPosition = writePosition.getHeaderPosition();
        long metadataPosition = writePosition.getMetadataPosition();
        long dataPosition = writePosition.getDataPosition();
        long nextHeaderPosition = writePosition.getNextHeaderPosition();

        // some sanity checks
        if (previousHeaderPosition != NOT_SET) {
            checkAligned(previousHeaderPosition, blockSize);
        }
        checkAligned(headerPosition, blockSize);
        checkAligned(nextHeaderPosition, blockSize);

        checkLogEntryPosition(headerPosition);

        long expectedMetadataPosition = headerPosition + blockSize;
        long expectedDataPosition = metadataPosition + metadataLength;
        long expectedNextHeaderPosition = up(dataPosition + dataLength, blockSize);
        checkState(metadataPosition == expectedMetadataPosition, "Metadata position was %s, expected %s", metadataPosition, expectedMetadataPosition);
        checkState(dataPosition == expectedDataPosition, "Data position was %s, expected %s", dataPosition, expectedDataPosition);
        checkState(nextHeaderPosition == expectedNextHeaderPosition, "Next header position was %s, expected %s", nextHeaderPosition, expectedNextHeaderPosition);
    }

    protected Optional<ChecksummedPositional<Header>> parseJournalHeader(ChecksummedPositional<byte[]> checksummedPositional) {
        try {
            if (checksummedPositional.isChecksumValid()) {
                Header indexBlock = parseFrom(checksummedPositional.getValue());
                return of(new ChecksummedPositional<Header>(checksummedPositional.getPosition(), indexBlock, checksummedPositional.getActualChecksum()) {
                    @Override
                    public boolean isChecksumValid() {
                        return true;
                    }
                });
            } else {
                LOGGER.warn("Invalid checksum for index block @ position " + checksummedPositional.getPosition());
                return absent();
            }
        } catch (Throwable e) {
            LOGGER.warn("Error parsing index block @ position " + checksummedPositional.getPosition(), e);
            return absent();
        }
    }

    protected Optional<ChecksummedPositional<Super>> parseSuperBlock(ChecksummedPositional<byte[]> checksummedPositional) {
        try {
            if (checksummedPositional.isChecksumValid()) {
                Super indexBlock = Super.parseFrom(checksummedPositional.getValue());
                return of(new ChecksummedPositional<Super>(checksummedPositional.getPosition(), indexBlock, checksummedPositional.getActualChecksum()) {
                    @Override
                    public boolean isChecksumValid() {
                        return true;
                    }
                });
            } else {
                LOGGER.warn("Invalid checksum for index block @ position " + checksummedPositional.getPosition());
                return absent();
            }
        } catch (Throwable e) {
            LOGGER.warn("Error parsing index block @ position " + checksummedPositional.getPosition(), e);
            return absent();
        }
    }

    public static class Entry {

        private final long headerPosition;
        private final Header header;
        private final BlobFile blobFile;

        public Entry(BlobFile blobFile, long headerPosition, Header header) {
            this.blobFile = blobFile;
            this.headerPosition = headerPosition;
            this.header = header;
        }

        public long getHeaderPosition() {
            return headerPosition;
        }

        public long getMetaDataPosition() {
            return header.getMetaDataPosition();
        }

        public long getMetaDataLength() {
            return header.getMetaDataLength();
        }

        public long getDataPosition() {
            return header.getDataPosition();
        }

        public long getDataLength() {
            return header.getDataLength();
        }

        public long getNextHeaderPosition() {
            return header.getNextHeaderPosition();
        }

        public long getPreviousHeaderPositon() {
            return header.getPreviousHeaderPositon();
        }

        public Observable<Buffer> getMetadata(SfsVertx vertx) {
            long metadataLength = header.getMetaDataLength();
            long metadataPosition = header.getMetaDataPosition();
            BufferWriteEndableWriteStream bufferWriteStreamConsumer = new BufferWriteEndableWriteStream();
            return blobFile.produce(vertx, metadataPosition, metadataLength, bufferWriteStreamConsumer)
                    .map(aVoid -> bufferWriteStreamConsumer.toBuffer());
        }

        public Observable<Void> produceData(SfsVertx vertx, BufferEndableWriteStream bufferStreamConsumer) {
            long dataLength = header.getDataLength();
            long dataPosition = header.getDataPosition();
            return blobFile.produce(vertx, dataPosition, dataLength, bufferStreamConsumer);
        }
    }

    private static class WritePosition {

        private final long previousHeaderPosition;
        private final long headerPosition;
        private final long nextHeaderPosition;
        private final long metadataPosition;
        private final long dataPosition;

        public WritePosition(long previousHeaderPosition, long headerPosition, long metadataPosition, long dataPosition, long nextHeaderPosition) {
            this.previousHeaderPosition = previousHeaderPosition;
            this.headerPosition = headerPosition;
            this.nextHeaderPosition = nextHeaderPosition;
            this.dataPosition = dataPosition;
            this.metadataPosition = metadataPosition;
        }

        public long getMetadataPosition() {
            return metadataPosition;
        }

        public long getPreviousHeaderPosition() {
            return previousHeaderPosition;
        }

        public long getHeaderPosition() {
            return headerPosition;
        }

        public long getNextHeaderPosition() {
            return nextHeaderPosition;
        }

        public long getDataPosition() {
            return dataPosition;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof WritePosition)) return false;
            WritePosition that = (WritePosition) o;
            return previousHeaderPosition == that.previousHeaderPosition &&
                    headerPosition == that.headerPosition &&
                    nextHeaderPosition == that.nextHeaderPosition &&
                    metadataPosition == that.metadataPosition &&
                    dataPosition == that.dataPosition;
        }

        @Override
        public int hashCode() {
            return hash(previousHeaderPosition, headerPosition, nextHeaderPosition, metadataPosition, dataPosition);
        }

        @Override
        public String toString() {
            return "WritePosition{" +
                    "previousHeaderPosition=" + previousHeaderPosition +
                    ", headerPosition=" + headerPosition +
                    ", metadataPosition=" + metadataPosition +
                    ", dataPosition=" + dataPosition +
                    ", nextHeaderPosition=" + nextHeaderPosition +
                    '}';
        }
    }
}
