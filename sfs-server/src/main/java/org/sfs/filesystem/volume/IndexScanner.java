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

import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;
import org.sfs.filesystem.ChecksummedPositional;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.math.LongMath.checkedAdd;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.TRUE;
import static org.sfs.block.RangeLock.lockedObservable;
import static org.sfs.filesystem.volume.IndexBlockReader.LockType;
import static org.sfs.filesystem.volume.IndexBlockReader.LockType.READ;
import static org.sfs.filesystem.volume.IndexBlockReader.LockType.WRITE;
import static org.sfs.math.Rounding.up;
import static org.sfs.protobuf.XVolume.XIndexBlock;
import static org.sfs.rx.RxHelper.iterate;

public class IndexScanner {

    private static final Logger LOGGER = getLogger(IndexScanner.class);
    private boolean isDebugEnabled;
    private final IndexFile indexFile;
    private final int blockSize;
    private final int batchSize;
    private final long lockWaitTimeout;
    private long position;
    private long blockCount = 0;

    public IndexScanner(IndexFile indexFile, int batchSize, long lockWaitTimeout) {
        this.isDebugEnabled = LOGGER.isDebugEnabled();
        this.indexFile = indexFile;
        this.blockSize = indexFile.getBlockSize();
        this.batchSize = batchSize;
        this.lockWaitTimeout = lockWaitTimeout;
        this.position = 0;
    }

    public Observable<Void> scanIndex(SfsVertx vertx, LockType lockType, Func1<ChecksummedPositional<XIndexBlock>, Observable<Void>> transformer) {
        return indexFile.size(vertx)
                .flatMap(fileSize -> {
                    ObservableFuture<Void> handler = RxHelper.observableFuture();
                    long roundedFileSize = up(fileSize, blockSize);
                    if (isDebugEnabled) {
                        long numberOfBlocks = roundedFileSize / blockSize;
                        LOGGER.debug("Max position is " + roundedFileSize + ". Expecting at most " + numberOfBlocks + " blocks");
                    }
                    scanIndex0(vertx, lockType, transformer, handler, roundedFileSize);
                    return handler;
                });
    }


    protected void scanIndex0(SfsVertx vertx, LockType lockType, Func1<ChecksummedPositional<XIndexBlock>, Observable<Void>> transformer, ObservableFuture<Void> handler, long fileSize) {
        int bufferSize = batchSize * blockSize;
        if (isDebugEnabled) {
            LOGGER.debug("Reading " + batchSize + " blocks @ position " + position);
        }
        if (READ.equals(lockType) || WRITE.equals(lockType)) {
            checkState(lockWaitTimeout > 0, "Invalid LockWaitTimeout value %s", lockWaitTimeout);
            lockedObservable(vertx,
                    () -> {
                        if (WRITE.equals(lockType)) {
                            return indexFile.tryWriteLock(position, bufferSize);
                        } else if (READ.equals(lockType)) {
                            return indexFile.tryReadLock(position, bufferSize);
                        } else {
                            throw new IllegalStateException("Unsupported Locked Type " + lockType);
                        }
                    },
                    () -> indexFile.getBlocks(vertx, position, batchSize),
                    lockWaitTimeout)
                    .flatMap(checksummedPositionals ->
                            iterate(vertx,
                                    checksummedPositionals,
                                    xIndexBlockChecksummedPositional ->
                                            transformer.call(xIndexBlockChecksummedPositional)
                                                    .doOnNext(aVoid -> blockCount++)
                                                    .map(aVoid -> TRUE)))
                    .doOnNext(aBoolean -> {
                        if (isDebugEnabled) {
                            LOGGER.debug("Scanned " + blockCount + " blocks");
                        }
                    })
                    .subscribe(new Subscriber<Boolean>() {
                        @Override
                        public void onCompleted() {
                            if (isDebugEnabled) {
                                LOGGER.debug("Scanned " + blockCount + " blocks");
                            }
                            position = checkedAdd(position, bufferSize);
                            if (position >= fileSize) {
                                handler.complete(null);
                            } else {
                                if (!isUnsubscribed()) {
                                    vertx.runOnContext(event -> scanIndex0(vertx, lockType, transformer, handler, fileSize));
                                }
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            handler.fail(e);
                        }

                        @Override
                        public void onNext(Boolean aBoolean) {

                        }
                    });
        } else {
            indexFile.getBlocks(vertx, position, batchSize)
                    .flatMap(checksummedPositionals ->
                            iterate(vertx,
                                    checksummedPositionals,
                                    xIndexBlockChecksummedPositional ->
                                            transformer.call(xIndexBlockChecksummedPositional)
                                                    .doOnNext(aVoid -> blockCount++)
                                                    .map(aVoid -> TRUE)))
                    .doOnNext(aBoolean -> {
                        if (isDebugEnabled) {
                            LOGGER.debug("Scanned " + blockCount + " blocks");
                        }
                    })
                    .subscribe(new Subscriber<Boolean>() {
                        @Override
                        public void onCompleted() {
                            position = checkedAdd(position, bufferSize);
                            if (position >= fileSize) {
                                handler.complete(null);
                            } else {
                                vertx.runOnContext(event -> scanIndex0(vertx, lockType, transformer, handler, fileSize));
                            }
                        }

                        @Override
                        public void onError(Throwable e) {
                            handler.fail(e);
                        }

                        @Override
                        public void onNext(Boolean aBoolean) {

                        }
                    });
        }
    }
}
