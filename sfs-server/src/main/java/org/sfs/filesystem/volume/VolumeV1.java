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

package org.sfs.filesystem.volume;


import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.math.LongMath;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.ReadStream;
import org.sfs.SfsVertx;
import org.sfs.block.RangeLock;
import org.sfs.block.RecyclingAllocator;
import org.sfs.filesystem.BlobFile;
import org.sfs.filesystem.ChecksummedPositional;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.protobuf.XVolume;
import org.sfs.rx.Defer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.util.ExceptionHelper;
import org.sfs.vo.TransientXAllocatedFile;
import org.sfs.vo.TransientXFileSystem;
import org.sfs.vo.TransientXVolume;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class VolumeV1 implements Volume {

    private enum GcState {
        PAUSED,
        EXECUTING,
        STOPPED
    }

    private final Logger logger;
    // This value needs to be set high enough so that that when MaintainObjectsForNode
    // makes a copy of an object it's able to ack this copy during it's next run interval.
    //
    // The object is not ack'd during the initial copy since the when I wrote the copy code
    // it was simpler to leave the object copy unacked until the next run interval. Acking the
    // object during the copy would require a subsequent update to the index which is awkward
    // to do since MaintainObjectsForNode updates the index using bulk api
    //
    public static final long MAX_GC_AGE = TimeUnit.DAYS.toMillis(4);
    public static final int INDEX_BLOCK_SIZE = 60;
    public static final int DATA_BLOCK_SIZE = 8;
    public static final int TINY_DATA_THRESHOLD = INDEX_BLOCK_SIZE + DATA_BLOCK_SIZE;
    private static final int INDEX_SCAN_BATCH_SIZE = 1000;
    private static final long ACTIVE_WRITE_STREAM_TIMEOUT = TimeUnit.MINUTES.toMillis(1);
    private static final long LOCK_WAIT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    private static final int MAX_FREE_RANGES = 100000;
    private Path metaFilePath;
    private Path dataFilePath;
    private Path indexFilePath;
    private Logger gcLogger;
    private RecyclingAllocator dataFileAllocator;
    private RecyclingAllocator indexFileAllocator;
    private final Path basePath;
    private String volumeId;
    private AtomicReference<GcState> gcState = new AtomicReference<>(GcState.STOPPED);
    private AtomicReference<Status> volumeState = new AtomicReference<>(Status.STOPPED);
    private MetaFile metaFile;
    private IndexFile indexFile;
    private BlobFile blobFile;
    private int indexBlockSize = -1;
    private int dataBlockSize = -1;

    public VolumeV1(Path path) {
        this.basePath = path;
        this.metaFilePath = metaFilePath(basePath);
        this.dataFilePath = dataFilePath(basePath);
        this.indexFilePath = indexFilePath(basePath);
        logger = LoggerFactory.getLogger(VolumeV1.class.getName() + "." + join(basePath));
        gcLogger = LoggerFactory.getLogger(VolumeV1.class.getName() + "." + join(basePath) + ".gc");
    }

    protected static String join(Path path) {
        return Joiner.on('.').join(path);
    }

    protected Path metaFilePath(Path basePath) {
        return Paths.get(basePath.toString(), "meta").normalize();
    }

    protected Path indexFilePath(Path basePath) {
        return Paths.get(basePath.toString(), "index").normalize();
    }

    protected Path dataFilePath(Path basePath) {
        return Paths.get(basePath.toString(), "data").normalize();
    }

    @Override
    public String getVolumeId() {
        return volumeId;
    }

    @Override
    public Status status() {
        return volumeState.get();
    }

    @Override
    public Observable<TransientXVolume> volumeInfo(SfsVertx vertx) {
        return Defer.aVoid()
                .doOnNext(aVoid -> {
                    checkStarted();
                })
                .flatMap(aVoid -> {
                    Context context = vertx.getOrCreateContext();
                    return RxHelper.executeBlocking(context, vertx.getBackgroundPool(), () -> {

                        try {
                            FileStore fileStore = Files.getFileStore(basePath);

                            long usableSpace = fileStore.getUsableSpace();
                            long actualUsableSpace;
                            try {
                                actualUsableSpace = LongMath.checkedAdd(indexFileAllocator.getBytesFree(usableSpace), dataFileAllocator.getBytesFree(usableSpace));
                            } catch (ArithmeticException e) {
                                actualUsableSpace = usableSpace;
                            }

                            TransientXAllocatedFile indexFileInfo = new TransientXAllocatedFile()
                                    .setFile(indexFilePath.toString())
                                    .setFileSizeBytes(Files.size(indexFilePath))
                                    .setBytesFree(indexFileAllocator.getBytesFree(usableSpace))
                                    .setFreeRangeCount(indexFileAllocator.getNumberOfFreeRanges())
                                    .setLockCount(indexFile.getLockCount())
                                    .setWriteQueueBytesPending(indexFile.getWriteQueueSize())
                                    .setWriteQueueBytesFull(indexFile.getWriteQueueMaxWrites())
                                    .setWriteQueueBytesDrained(indexFile.getWriteQueueLowWater());

                            TransientXAllocatedFile dataFileInfo = new TransientXAllocatedFile()
                                    .setFile(dataFilePath.toString())
                                    .setFileSizeBytes(Files.size(dataFilePath))
                                    .setBytesFree(dataFileAllocator.getBytesFree(usableSpace))
                                    .setFreeRangeCount(dataFileAllocator.getNumberOfFreeRanges())
                                    .setLockCount(blobFile.getLockCount())
                                    .setWriteQueueBytesPending(blobFile.getWriteQueueSize())
                                    .setWriteQueueBytesFull(blobFile.getWriteQueueMaxWrites())
                                    .setWriteQueueBytesDrained(blobFile.getWriteQueueLowWater());


                            TransientXFileSystem fileSystemInfo = new TransientXFileSystem()
                                    .setDevice(fileStore.name())
                                    .setPath(basePath.toString())
                                    .setTotalSpace(fileStore.getTotalSpace())
                                    .setUnallocatedSpace(fileStore.getUnallocatedSpace())
                                    .setUsableSpace(usableSpace)
                                    .setType(fileStore.type())
                                    .setPartition(basePath.getRoot().toString());


                            TransientXVolume volumeInfo = new TransientXVolume()
                                    .setId(volumeId)
                                    .setIndexFile(indexFileInfo)
                                    .setDataFile(dataFileInfo)
                                    .setFileSystem(fileSystemInfo)
                                    .setUsableSpace(actualUsableSpace)
                                    .setStatus(volumeState.get());

                            return volumeInfo;

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                });
    }

    @Override
    public Observable<Void> copy(SfsVertx vertx, Path destinationDirectory) {
        Observable<Void> o =
                Defer.aVoid()
                        .doOnNext(aVoid -> {
                            checkStarted();
                        })
                        .doOnNext(aVoid -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Waiting for gc pause for copy of " + basePath + " to " + destinationDirectory);
                            }
                        })
                        .flatMap(aVoid -> waitAndPauseGc(vertx))
                        .doOnNext(aVoid -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Gc paused for copy of " + basePath + " to " + destinationDirectory);
                            }
                        })
                        .doOnNext(aVoid -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Started copy of " + basePath + " to " + destinationDirectory);
                            }
                        })
                        .flatMap(aVoid -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Creating directory " + destinationDirectory);
                            }
                            ObservableFuture<Void> handler = RxHelper.observableFuture();
                            vertx.fileSystem().mkdirs(destinationDirectory.toString(), null, handler.toHandler());
                            return handler
                                    .map(aVoid1 -> {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("Created directory " + destinationDirectory);
                                        }
                                        return (Void) null;
                                    });
                        })
                        .flatMap(aVoid -> {
                            MetaFile dstMetaFile = new MetaFile(metaFilePath(destinationDirectory));
                            IndexFile dstIndexFile = new IndexFile(indexFilePath(destinationDirectory), indexBlockSize);
                            BlobFile dstBlobFile = new BlobFile(dataFilePath(destinationDirectory), dataBlockSize, ACTIVE_WRITE_STREAM_TIMEOUT);
                            return Observable.just((Void) null)
                                    .flatMap(aVoid1 -> dstMetaFile.open(vertx, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
                                    .flatMap(aVoid1 -> dstIndexFile.open(vertx, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
                                    .flatMap(aVoid1 -> dstBlobFile.open(vertx, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
                                    .flatMap(aVoid1 -> dstMetaFile.enableWrites(vertx))
                                    .flatMap(aVoid1 -> dstIndexFile.enableWrites(vertx))
                                    .flatMap(aVoid1 -> dstBlobFile.enableWrites(vertx))
                                    .flatMap(aVoid1 ->
                                            getSuperBlock(vertx)
                                                    .flatMap(xSuperBlock -> dstMetaFile.set(vertx, xSuperBlock)))
                                    .flatMap(aVoid1 ->
                                            scanIndex(vertx, IndexBlockReader.LockType.READ, checksummedPositional -> {
                                                XVolume.XIndexBlock header = checksummedPositional.getValue();
                                                return dstIndexFile.setBlock(vertx, checksummedPositional.getPosition(), checksummedPositional.getValue())
                                                        .flatMap(aVoid2 -> blobFile.copy(vertx, header.getDataPosition(), header.getDataLength(), dstBlobFile, header.getDataPosition(), header.getDataLength()))
                                                        .singleOrDefault(null);
                                            }))
                                    .flatMap(aVoid1 ->
                                            dstMetaFile.disableWrites(vertx))
                                    .flatMap(aVoid1 ->
                                            dstIndexFile.disableWrites(vertx))
                                    .flatMap(aVoid1 ->
                                            dstBlobFile.disableWrites(vertx))
                                    .flatMap(aVoid1 ->
                                            dstMetaFile.force(vertx, true))
                                    .flatMap(aVoid1 ->
                                            dstIndexFile.force(vertx, true))
                                    .flatMap(aVoid1 ->
                                            dstBlobFile.force(vertx, true))
                                    .flatMap(aVoid1 ->
                                            dstMetaFile.close(vertx))
                                    .flatMap(aVoid1 ->
                                            dstIndexFile.close(vertx))
                                    .flatMap(aVoid1 ->
                                            dstBlobFile.close(vertx));
                        })
                        .doOnNext(aVoid -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Finished copy of " + basePath + " to " + destinationDirectory);
                            }
                        });
        AtomicBoolean resumed = new AtomicBoolean(false);
        return Observable.using(
                () -> null,
                aVoid -> o
                        .single()
                        .doOnNext(aVoid1 -> {
                            resumed.compareAndSet(false, true);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Resume gc for copy of " + basePath + " to " + destinationDirectory);
                            }
                        }),
                aVoid -> {
                    if (!resumed.get()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Resume gc for copy of " + basePath + " to " + destinationDirectory);
                        }
                        resumeGc();
                    }
                },
                true)
                .onErrorResumeNext(throwable -> {
                    Optional<RejectedExecutionException> oException = ExceptionHelper.unwrapCause(RejectedExecutionException.class, throwable);
                    if (oException.isPresent()) {
                        return Observable.error(new VolumeToBusyExecutionException(oException.get()));
                    } else {
                        return Observable.error(throwable);
                    }
                });
    }

    protected Observable<XVolume.XSuperBlock> getSuperBlock(SfsVertx vertx) {
        return RangeLock.lockedObservable(vertx,
                () -> metaFile.tryReadLock(),
                () -> metaFile.getBlock(vertx).map(Optional::get),
                LOCK_WAIT_TIMEOUT);
    }

    protected Observable<Void> setSuperBlock(SfsVertx vertx, XVolume.XSuperBlock xSuperBlock) {
        return RangeLock.lockedObservable(vertx,
                () -> metaFile.tryWriteLock(),
                () -> metaFile.set(vertx, xSuperBlock),
                LOCK_WAIT_TIMEOUT)
                .flatMap(aVoid -> metaFile.force(vertx, false));
    }

    @Override
    public Observable<Void> open(SfsVertx vertx) {
        final VolumeV1 _this = this;;
        return Defer.aVoid()
                .doOnNext(aVoid -> Preconditions.checkState(volumeState.compareAndSet(Status.STOPPED, Status.STARTING)))
                .doOnNext(aVoid -> logger.info("Starting volume " + basePath.toString()))
                .flatMap(aVoid -> {
                    Context context = vertx.getOrCreateContext();
                    return RxHelper.executeBlocking(context, vertx.getBackgroundPool(), () -> {
                        try {

                            Files.createDirectories(basePath);

                            metaFilePath = metaFilePath(basePath).normalize();
                            dataFilePath = dataFilePath(basePath).normalize();
                            indexFilePath = indexFilePath(basePath).normalize();

                            return (Void) null;

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                })
                .doOnNext(aVoid -> logger.info("Starting Metadata Initialization"))
                .doOnNext(aVoid -> metaFile = new MetaFile(metaFilePath))
                .flatMap(aVoid -> metaFile.open(vertx, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
                .flatMap(aVoid ->
                        metaFile.size(vertx)
                                .flatMap(size -> {
                                    if (size <= 0) {
                                        _this.volumeId = UUID.randomUUID().toString();
                                        _this.dataBlockSize = DATA_BLOCK_SIZE;
                                        _this.indexBlockSize = INDEX_BLOCK_SIZE;
                                        XVolume.XSuperBlock xSuperBlock =
                                                XVolume.XSuperBlock.newBuilder()
                                                        .setVolumeId(_this.volumeId)
                                                        .setDataBlockSize(dataBlockSize)
                                                        .setIndexBlockSize(indexBlockSize)
                                                        .build();
                                        return metaFile.enableWrites(vertx)
                                                .flatMap(aVoid1 -> setSuperBlock(vertx, xSuperBlock));
                                    } else {
                                        return metaFile.getBlock(vertx)
                                                .map(Optional::get)
                                                .map(superBlock -> {
                                                    Preconditions.checkState(superBlock.getType() != null, "Corrupt superblock");
                                                    Preconditions.checkState(superBlock.getVolumeId() != null, "Corrupt superblock");
                                                    Preconditions.checkState(superBlock.getDataBlockSize() > 0, "Corrupt superblock");
                                                    Preconditions.checkState(superBlock.getIndexBlockSize() > 0, "Corrupt superblock");
                                                    _this.volumeId = superBlock.getVolumeId();
                                                    _this.indexBlockSize = superBlock.getIndexBlockSize();
                                                    _this.dataBlockSize = superBlock.getDataBlockSize();
                                                    // TODO: add functionality to upgrade the volume data structures
                                                    Preconditions.checkState(_this.indexBlockSize == INDEX_BLOCK_SIZE, "Index block size %s does not match the expected block size for this volume %s", _this.indexBlockSize, INDEX_BLOCK_SIZE);
                                                    Preconditions.checkState(_this.dataBlockSize == DATA_BLOCK_SIZE, "Index block size %s does not match the expected block size for this volume %s", _this.dataBlockSize, DATA_BLOCK_SIZE);
                                                    return (Void) null;
                                                })
                                                .flatMap(aVoid1 -> metaFile.enableWrites(vertx));
                                    }
                                }))
                .doOnNext(aVoid -> logger.info("Finished Metadata Initialization"))
                .doOnNext(aVoid -> {
                    dataFileAllocator =
                            new RecyclingAllocator(dataBlockSize);
                    indexFileAllocator =
                            new RecyclingAllocator(indexBlockSize);

                    indexFile = new IndexFile(indexFilePath, indexBlockSize);
                    blobFile = new BlobFile(dataFilePath, dataBlockSize, ACTIVE_WRITE_STREAM_TIMEOUT);
                })
                .flatMap(aVoid -> indexFile.open(vertx, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
                .flatMap(aVoid -> blobFile.open(vertx, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
                .doOnNext(aVoid -> logger.info("Started Allocator Initialization"))
                .flatMap(aVoid ->
                        scanIndex(vertx, IndexBlockReader.LockType.NONE, checksummedPositional -> {
                            XVolume.XIndexBlock header = checksummedPositional.getValue();
                            // Allocate all records that haven't been marked as garbage collected
                            if (!header.getGarbageCollected()) {

                                final long headerPosition = checksummedPositional.getPosition();
                                long dataPosition = header.getDataPosition();
                                long dataLength = header.getDataLength();

                                long allocatedIndexPosition = indexFileAllocator.alloc(headerPosition, indexBlockSize);
                                long allocatedDataPosition = dataFileAllocator.alloc(dataPosition, dataLength);

                                if (allocatedDataPosition != dataPosition) {
                                    Preconditions.checkState(allocatedDataPosition == dataPosition, "Data position was %s, expected %s, block allocator was %s", allocatedDataPosition, dataPosition, dataFileAllocator.toString());
                                }
                                if (allocatedIndexPosition != headerPosition) {
                                    Preconditions.checkState(allocatedIndexPosition == headerPosition, "Header position was %s, expected %s, block allocator was %s", allocatedIndexPosition, headerPosition, indexFileAllocator.toString());
                                }
                            }
                            return Observable.just(null);
                        }))
                .doOnNext(aVoid -> logger.info("Finished Allocator Initialization"))
                .flatMap(aVoid -> indexFile.enableWrites(vertx))
                .flatMap(aVoid -> blobFile.enableWrites(vertx))
                .doOnNext(aVoid -> logger.info("Starting Garbage Collector Initialization"))
                .map(aVoid -> {
                    final long interval = TimeUnit.MINUTES.toMillis(1);
                    Handler<Long> handler = new Handler<Long>() {

                        Handler<Long> _this = this;

                        @Override
                        public void handle(Long event) {
                            garbageCollection(vertx)
                                    .count()
                                    .map(new ToVoid<>())
                                    .singleOrDefault(null)
                                    .subscribe(new Subscriber<Void>() {
                                        @Override
                                        public void onCompleted() {
                                            vertx.setTimer(interval, _this);
                                        }

                                        @Override
                                        public void onError(Throwable e) {
                                            vertx.setTimer(interval, _this);
                                        }

                                        @Override
                                        public void onNext(Void aVoid) {
                                            // do nothing
                                        }
                                    });
                        }
                    };
                    vertx.setTimer(interval, handler);
                    return (Void) null;
                })
                .doOnNext(aVoid -> logger.info("Finished Garbage Collector Initialization"))
                .doOnNext(aVoid -> Preconditions.checkState(volumeState.compareAndSet(Status.STARTING, Status.STARTED)))
                .doOnNext(aVoid -> {
                    logger.info("Started volume " + basePath.toString());
                });
    }


    @Override
    public Observable<Void> close(SfsVertx vertx) {
        return Defer.aVoid()
                .doOnNext(aVoid -> {
                    logger.info("Stopping volume " + basePath.toString());
                    Preconditions.checkState(volumeState.compareAndSet(Status.STARTED, Status.STOPPING));
                })
                .flatMap(aVoid -> waitAndPauseGc(vertx))
                .onErrorResumeNext(throwable -> {
                    logger.error("Handling error", throwable);
                    return Observable.just(null);
                })
                .flatMap(aVoid -> {
                    if (metaFile != null) {
                        return metaFile.disableWrites(vertx)
                                .flatMap(aVoid1 -> metaFile.force(vertx, true))
                                .flatMap(aVoid1 -> metaFile.close(vertx));
                    }
                    return Observable.just(null);
                })
                .onErrorResumeNext(throwable -> {
                    logger.error("Handling error", throwable);
                    return Observable.just(null);
                })
                .flatMap(aVoid -> {
                    if (indexFile != null) {
                        return indexFile.disableWrites(vertx)
                                .flatMap(aVoid1 -> indexFile.force(vertx, true))
                                .flatMap(aVoid1 -> indexFile.close(vertx));
                    }
                    return Observable.just(null);
                })
                .onErrorResumeNext(throwable -> {
                    logger.error("Handling error", throwable);
                    return Observable.just(null);
                })
                .flatMap(aVoid -> {
                    if (blobFile != null) {
                        return blobFile.disableWrites(vertx)
                                .flatMap(aVoid1 -> blobFile.force(vertx, true))
                                .flatMap(aVoid1 -> blobFile.close(vertx));
                    }
                    return Observable.just(null);
                })
                .onErrorResumeNext(throwable -> {
                    logger.error("Handling error", throwable);
                    return Observable.just(null);
                })
                .doOnNext(aVoid -> Preconditions.checkState(volumeState.compareAndSet(Status.STOPPING, Status.STOPPED)))
                .doOnNext(aVoid -> {
                    logger.info("Stopped volume " + basePath.toString());
                });
    }

    @Override
    public Observable<Optional<ReadStreamBlob>> getDataStream(SfsVertx vertx, final long position, final Optional<Long> oOffset, final Optional<Long> oLength) {
        return Defer.aVoid()
                .doOnNext(aVoid -> checkStarted())
                .flatMap(aVoid ->
                        RangeLock.lockedObservable(vertx,
                                () -> indexFile.tryReadLock(position, indexBlockSize),
                                () -> getIndexBlock0(vertx, position),
                                LOCK_WAIT_TIMEOUT))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(positional -> !positional.getValue().getGarbageCollected())
                .filter(positional -> !positional.getValue().getDeleted())
                .flatMap(positional -> {
                    XVolume.XIndexBlock header = positional.getValue();
                    long dataPosition = header.getDataPosition();
                    long dataLength = header.getDataLength();
                    long startPosition;
                    if (oOffset.isPresent()) {
                        long offset = oOffset.get();
                        startPosition = offset <= 0 ? dataPosition : LongMath.checkedAdd(dataPosition, offset);
                    } else {
                        startPosition = dataPosition;
                    }

                    long normalizedLength;
                    if (oLength.isPresent()) {
                        long length = oLength.get();
                        normalizedLength = length <= -1 ? dataLength : Math.min(dataLength, length);
                    } else {
                        normalizedLength = dataLength;
                    }

                    long endPosition = LongMath.checkedAdd(dataPosition, dataLength);
                    if (startPosition >= endPosition) {
                        Preconditions.checkState(startPosition <= endPosition, "Offset must be <= %s", endPosition - dataPosition);
                    }
                    ReadStreamBlob readStreamBlob =
                            new ReadStreamBlob(volumeId, position, 0, normalizedLength) {
                                @Override
                                public Observable<Void> produce(BufferEndableWriteStream endableWriteStream) {
                                    return blobFile.produce(vertx, startPosition, normalizedLength, endableWriteStream)
                                            .onErrorResumeNext(throwable -> {
                                                Optional<RejectedExecutionException> oException = ExceptionHelper.unwrapCause(RejectedExecutionException.class, throwable);
                                                if (oException.isPresent()) {
                                                    return Observable.error(new VolumeToBusyExecutionException(oException.get()));
                                                } else {
                                                    return Observable.error(throwable);
                                                }
                                            });
                                }
                            };
                    return Observable.just(Optional.of(readStreamBlob));
                })
                .singleOrDefault(Optional.absent())
                .onErrorResumeNext(throwable -> {
                    Optional<RejectedExecutionException> oException = ExceptionHelper.unwrapCause(RejectedExecutionException.class, throwable);
                    if (oException.isPresent()) {
                        return Observable.error(new VolumeToBusyExecutionException(oException.get()));
                    } else {
                        return Observable.error(throwable);
                    }
                });
    }

    @Override
    public Observable<WriteStreamBlob> putDataStream(SfsVertx vertx, final long length) {
        return Defer.aVoid()
                .doOnNext(aVoid -> {
                    checkStarted();
                    Preconditions.checkArgument(length >= 0, "Length must be >= 0");
                })
                .flatMap(aVoid -> allocate(length))
                .flatMap(allocatedPosition -> {
                    final long headerPosition = allocatedPosition.getHeaderPosition();
                    return RangeLock.lockedObservable(
                            vertx,
                            () -> indexFile.tryWriteLock(headerPosition, indexBlockSize),
                            () -> Observable.defer(() -> {
                                final long dataPosition = allocatedPosition.getDataPosition();

                                XVolume.XIndexBlock xHeader =
                                        XVolume.XIndexBlock.newBuilder()
                                                .setDataLength(length)
                                                .setAcknowledged(false)
                                                .setUpdatedTs(System.currentTimeMillis())
                                                .setDeleted(false)
                                                .setGarbageCollected(false)
                                                .setDataPosition(dataPosition)
                                                .build();
                                return setIndexBlock0(vertx, headerPosition, xHeader)
                                        .onErrorResumeNext(throwable -> {
                                            return deallocateHeaderAndData(headerPosition, dataPosition, length)
                                                    .map(aVoid1 -> {
                                                        if (throwable instanceof RuntimeException) {
                                                            throw (RuntimeException) throwable;
                                                        } else {
                                                            throw new RuntimeException(throwable);
                                                        }
                                                    });
                                        })
                                        .map(aVoid -> {
                                            WriteStreamBlob writeStreamBlob = new WriteStreamBlob(volumeId, headerPosition, length) {

                                                @Override
                                                public Observable<Void> consume(ReadStream<Buffer> src) {
                                                    return Defer.aVoid()
                                                            .flatMap(aVoid1 -> blobFile.consume(vertx, dataPosition, length, src))
                                                            .flatMap(aVoid1 -> blobFile.force(vertx, false))
                                                            .onErrorResumeNext(throwable -> {
                                                                Optional<RejectedExecutionException> oException = ExceptionHelper.unwrapCause(RejectedExecutionException.class, throwable);
                                                                if (oException.isPresent()) {
                                                                    return Observable.error(new VolumeToBusyExecutionException(oException.get()));
                                                                } else {
                                                                    return Observable.error(throwable);
                                                                }
                                                            });
                                                }
                                            };
                                            return writeStreamBlob;
                                        });
                            }),
                            LOCK_WAIT_TIMEOUT)
                            .flatMap(writeStreamBlob ->
                                    indexFile.force(vertx, false)
                                            .map(aVoid1 -> writeStreamBlob));
                })
                .onErrorResumeNext(throwable -> {
                    Optional<RejectedExecutionException> oException = ExceptionHelper.unwrapCause(RejectedExecutionException.class, throwable);
                    if (oException.isPresent()) {
                        return Observable.error(new VolumeToBusyExecutionException(oException.get()));
                    } else {
                        return Observable.error(throwable);
                    }
                });
    }

    @Override
    public Observable<Optional<HeaderBlob>> acknowledge(SfsVertx vertx, final long position) {
        return Defer.aVoid()
                .doOnNext(aVoid -> checkStarted())
                .flatMap(aVoid ->
                        RangeLock.lockedObservable(
                                vertx,
                                () -> indexFile.tryWriteLock(position, indexBlockSize),
                                () -> Defer.aVoid()
                                        .flatMap(aVoid2 -> getIndexBlock0(vertx, position))
                                        .filter(Optional::isPresent)
                                        .map(Optional::get)
                                        .filter(positional -> !positional.getValue().getGarbageCollected())
                                        .filter(positional -> !positional.getValue().getDeleted())
                                        .flatMap(positional -> {
                                            XVolume.XIndexBlock header = positional.getValue();
                                            XVolume.XIndexBlock updated = header.toBuilder()
                                                    .setAcknowledged(true)
                                                    .setDeleted(false)
                                                    .setUpdatedTs(System.currentTimeMillis())
                                                    .build();
                                            return setIndexBlock0(vertx, positional.getPosition(), updated)
                                                    .map(aVoid1 -> Optional.of(new HeaderBlob(volumeId, position, header.getDataLength())));
                                        })
                                        .singleOrDefault(Optional.absent()),
                                LOCK_WAIT_TIMEOUT
                        ).flatMap(headerBlobOptional ->
                                indexFile.force(vertx, false)
                                        .map(aVoid1 -> headerBlobOptional))
                )
                .onErrorResumeNext(throwable -> {
                    Optional<RejectedExecutionException> oException = ExceptionHelper.unwrapCause(RejectedExecutionException.class, throwable);
                    if (oException.isPresent()) {
                        return Observable.error(new VolumeToBusyExecutionException(oException.get()));
                    } else {
                        return Observable.error(throwable);
                    }
                });
    }

    @Override
    public Observable<Optional<HeaderBlob>> delete(SfsVertx vertx, final long position) {
        return Defer.aVoid()
                .doOnNext(aVoid -> checkStarted())
                .flatMap(aVoid ->
                        RangeLock.lockedObservable(vertx,
                                () -> indexFile.tryWriteLock(position, indexBlockSize),
                                () -> getIndexBlock0(vertx, position)
                                        .filter(Optional::isPresent)
                                        .map(Optional::get)
                                        .filter(positional -> !positional.getValue().getGarbageCollected())
                                        .flatMap(positional -> {
                                            XVolume.XIndexBlock header = positional.getValue();
                                            if (header.getDeleted()) {
                                                return Observable.just(Optional.of(new HeaderBlob(volumeId, position, header.getDataLength())));
                                            } else {
                                                XVolume.XIndexBlock updated = header.toBuilder()
                                                        .setDeleted(true)
                                                        .setUpdatedTs(System.currentTimeMillis())
                                                        .build();
                                                return setIndexBlock0(vertx, positional.getPosition(), updated)
                                                        .map(aVoid1 -> Optional.of(new HeaderBlob(volumeId, position, header.getDataLength())));
                                            }
                                        })
                                        .singleOrDefault(Optional.absent()),
                                LOCK_WAIT_TIMEOUT)
                                .flatMap(headerBlobOptional ->
                                        indexFile.force(vertx, false)
                                                .map(aVoid1 -> headerBlobOptional))
                )
                .onErrorResumeNext(throwable -> {
                    Optional<RejectedExecutionException> oException = ExceptionHelper.unwrapCause(RejectedExecutionException.class, throwable);
                    if (oException.isPresent()) {
                        return Observable.error(new VolumeToBusyExecutionException(oException.get()));
                    } else {
                        return Observable.error(throwable);
                    }
                });

    }

    protected Observable<Optional<ChecksummedPositional<XVolume.XIndexBlock>>> getIndexBlock0(SfsVertx vertx, final long position) {
        return indexFile.getBlock(vertx, position);
    }

    protected Observable<Void> setIndexBlock0(SfsVertx vertx, final long position, final XVolume.XIndexBlock header) {
        return indexFile.setBlock(vertx, position, header);
    }

    protected void checkAligned(long value, int blockSize) {
        Preconditions.checkState(value % blockSize == 0, "%s is not multiple of %s", value, blockSize);
    }


    private void checkStarted() {
        if (!Status.STARTED.equals(volumeState.get())) {
            throw new VolumeStoppedException();
        }
    }

    protected Observable<Void> garbageCollection(SfsVertx vertx) {

        return Defer.aVoid()
                .doOnNext(aVoid -> checkStarted())
                .flatMap(aVoid -> {
                    AtomicBoolean stopped = new AtomicBoolean(false);
                    return Observable.using(
                            () -> gcState.compareAndSet(GcState.STOPPED, GcState.EXECUTING),
                            locked -> {
                                if (Boolean.TRUE.equals(locked)) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("Started Garbage Collection " + basePath.toString());
                                    }
                                    return scanIndex(vertx, IndexBlockReader.LockType.READ, checksummedPositional -> {
                                        checkStarted();
                                        XVolume.XIndexBlock xHeader = checksummedPositional.getValue();
                                        boolean shouldDeallocate = false;
                                        if (xHeader.getUpdatedTs() > 0) {
                                            long age = System.currentTimeMillis() - xHeader.getUpdatedTs();
                                            if (age >= MAX_GC_AGE
                                                    && !xHeader.getGarbageCollected()
                                                    && (xHeader.getDeleted()
                                                    || !xHeader.getAcknowledged())) {
                                                shouldDeallocate = true;
                                            }
                                        }
                                        if (shouldDeallocate
                                                && indexFileAllocator.getNumberOfFreeRanges() < MAX_FREE_RANGES
                                                && dataFileAllocator.getNumberOfFreeRanges() < MAX_FREE_RANGES) {

                                            final long headerPosition = checksummedPositional.getPosition();
                                            long dataPosition = xHeader.getDataPosition();
                                            long dataLength = xHeader.getDataLength();


                                            if (gcLogger.isDebugEnabled()) {
                                                gcLogger.debug(String.format("GC Recycle Blocks {%s/%d/%d %d/%d} = {%s}", BaseEncoding.base64().encode(checksummedPositional.getActualChecksum()), headerPosition, indexBlockSize, dataPosition, dataLength, xHeader));
                                            }

                                            return RangeLock.lockedObservable(vertx,
                                                    () -> indexFile.tryWriteLock(headerPosition, indexBlockSize),
                                                    () -> Defer.aVoid()
                                                            .flatMap(aVoid1 -> getIndexBlock0(vertx, headerPosition))
                                                            .map(Optional::get)
                                                            .filter(optimisticLockChecksummedPositional -> Arrays.equals(checksummedPositional.getActualChecksum(), optimisticLockChecksummedPositional.getActualChecksum()))
                                                            .flatMap(optimisticLockChecksummedPositional -> {
                                                                XVolume.XIndexBlock updated =
                                                                        xHeader.toBuilder()
                                                                                .setUpdatedTs(System.currentTimeMillis())
                                                                                .setGarbageCollected(true)
                                                                                .setDeleted(true)
                                                                                .build();
                                                                return setIndexBlock0(vertx, headerPosition, updated);
                                                            })
                                                            .flatMap(aVoid1 -> deallocateHeaderAndData(headerPosition, dataPosition, dataLength))
                                                            .singleOrDefault(null),
                                                    LOCK_WAIT_TIMEOUT);
                                        } else {
                                            return Observable.just(null);
                                        }
                                    }).flatMap(aVoid1 ->
                                            indexFile.force(vertx, false)
                                    ).doOnNext(aVoid1 -> {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("Finished Garbage Collection " + basePath.toString());
                                        }
                                        stopped.compareAndSet(false, true);
                                        Preconditions.checkState(gcState.compareAndSet(GcState.EXECUTING, GcState.STOPPED), "Concurrent modification");
                                    });
                                } else {
                                    return Observable.just(null);
                                }
                            },
                            locked -> {
                                if (locked && !stopped.get()) {
                                    Preconditions.checkState(gcState.compareAndSet(GcState.EXECUTING, GcState.STOPPED), "Concurrent modification");
                                }
                            },
                            true
                    );
                });
    }


    protected Observable<AllocatedPosition> allocate(final long dataLength) {
        return Observable.defer(() -> {
            long indexPosition = indexFileAllocator.allocNextAvailable(indexBlockSize);
            long dataPosition = dataFileAllocator.allocNextAvailable(dataLength);
            AllocatedPosition allocatedPosition = new AllocatedPosition()
                    .setHeaderPosition(indexPosition)
                    .setDataPosition(dataPosition);
            return Observable.just(allocatedPosition);
        });
    }

    protected Observable<Void> deallocateHeaderAndData(final long headerPosition, final long dataPosition, final long dataLength) {
        return Observable.defer(() -> {
            checkAligned(dataPosition, dataBlockSize);
            checkAligned(headerPosition, indexBlockSize);
            dataFileAllocator.free(dataPosition, dataLength);
            indexFileAllocator.free(headerPosition, indexBlockSize);
            return Observable.just(null);
        });
    }


    protected Observable<Void> scanIndex(SfsVertx vertx, IndexBlockReader.LockType lockType, Func1<ChecksummedPositional<XVolume.XIndexBlock>, Observable<Void>> transformer) {
        IndexScanner indexScanner = new IndexScanner(indexFile, INDEX_SCAN_BATCH_SIZE, LOCK_WAIT_TIMEOUT);
        return indexScanner.scanIndex(vertx, lockType, transformer);
    }

    protected static class AllocatedPosition {

        private long headerPosition;
        private long dataPosition;

        public AllocatedPosition() {
        }

        public AllocatedPosition setDataPosition(long dataPosition) {
            this.dataPosition = dataPosition;
            return this;
        }

        public AllocatedPosition setHeaderPosition(long headerPosition) {
            this.headerPosition = headerPosition;
            return this;
        }

        public long getHeaderPosition() {
            return headerPosition;
        }

        public long getDataPosition() {
            return dataPosition;
        }
    }

    protected void resumeGc() {
        Preconditions.checkState(gcState.compareAndSet(GcState.PAUSED, GcState.STOPPED), "Concurrent Gc State Toggle");
    }

    protected Observable<Void> waitAndPauseGc(SfsVertx vertx) {
        return Observable.defer(() -> {
            ObservableFuture<Void> observableHandler = RxHelper.observableFuture();
            waitAndPauseGc0(vertx, observableHandler);
            return observableHandler;
        });
    }

    protected void waitAndPauseGc0(SfsVertx vertx, ObservableFuture<Void> handler) {
        if (gcState.compareAndSet(GcState.EXECUTING, GcState.PAUSED)
                || gcState.compareAndSet(GcState.STOPPED, GcState.PAUSED)
                || gcState.compareAndSet(GcState.PAUSED, GcState.PAUSED)) {
            handler.complete(null);
        } else {
            vertx.setTimer(10, event -> waitAndPauseGc0(vertx, handler));
        }
    }
}

