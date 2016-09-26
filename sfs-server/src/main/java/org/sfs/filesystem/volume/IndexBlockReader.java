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

package org.sfs.filesystem.volume;

import com.google.common.collect.Lists;
import org.sfs.SfsVertx;
import org.sfs.filesystem.ChecksummedPositional;
import rx.Observable;
import rx.Producer;
import rx.Subscriber;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.MAX_VALUE;
import static org.sfs.block.RangeLock.lockedObservable;
import static org.sfs.filesystem.volume.IndexBlockReader.LockType.NONE;
import static org.sfs.filesystem.volume.IndexBlockReader.LockType.READ;
import static org.sfs.filesystem.volume.IndexBlockReader.LockType.WRITE;
import static org.sfs.protobuf.XVolume.XIndexBlock;
import static rx.Observable.create;
import static rx.internal.operators.BackpressureUtils.getAndAddRequest;

public class IndexBlockReader {

    public enum LockType {
        READ,
        WRITE,
        NONE
    }

    private final SfsVertx vertx;
    private long position = 0;
    private final long lastBlockPosition;
    private final int batchSize;
    private final int readSize;
    private IndexFile blockFile;
    private LockType lockType = NONE;
    private long lockWaitTimeout = -1;
    private boolean hasNext = true;

    public IndexBlockReader(SfsVertx vertx, IndexFile blockFile, int blockSize, int batchSize, long lastBlockPosition) {
        this.vertx = vertx;
        this.blockFile = blockFile;
        this.batchSize = batchSize;
        this.readSize = blockSize * batchSize;
        this.lastBlockPosition = lastBlockPosition;
    }

    public IndexBlockReader enableLocking(LockType lockType, long lockWaitTimeout) {
        this.lockType = lockType;
        this.lockWaitTimeout = lockWaitTimeout;
        return this;
    }

    public Observable<Iterable<ChecksummedPositional<XIndexBlock>>> toObservable() {
        return create(new OnSubscribeFromBlockReader<>(this));
    }

    protected boolean hasNext() {
        return hasNext;
    }

    protected Observable<? extends List<ChecksummedPositional<XIndexBlock>>> next() {
        if (READ.equals(lockType) || WRITE.equals(lockType)) {
            checkState(lockWaitTimeout > 0, "Invalid LockWaitTimeout value %s", lockWaitTimeout);
            return lockedObservable(vertx,
                    () -> {
                        if (WRITE.equals(lockType)) {
                            return blockFile.tryWriteLock(position, readSize);
                        } else if (READ.equals(lockType)) {
                            return blockFile.tryReadLock(position, readSize);
                        } else {
                            throw new IllegalStateException("Unsupported Locked Type " + lockType);
                        }
                    },
                    () -> blockFile.getBlocks(vertx, position, batchSize)
                            .map(Lists::newArrayList)
                            .doOnNext(checksummedPositionals -> hasNext = !checksummedPositionals.isEmpty())
                            .doOnNext(checksummedPositionals -> position = position + readSize),
                    lockWaitTimeout);
        } else {
            return blockFile.getBlocks(vertx, position, batchSize)
                    .map(Lists::newArrayList)
                    .doOnNext(checksummedPositionals -> hasNext = !checksummedPositionals.isEmpty())
                    .doOnNext(checksummedPositionals -> position = position + readSize);
        }
    }

    protected static class OnSubscribeFromBlockReader<T extends Iterable<ChecksummedPositional<XIndexBlock>>> implements Observable.OnSubscribe<T> {

        final IndexBlockReader blockReader;

        public OnSubscribeFromBlockReader(IndexBlockReader iterable) {
            if (iterable == null) {
                throw new NullPointerException("iterable must not be null");
            }
            this.blockReader = iterable;
        }

        @Override
        public void call(Subscriber<? super T> o) {
            if (!blockReader.hasNext() && !o.isUnsubscribed()) {
                o.onCompleted();
            } else {
                o.setProducer(new BlockReaderProducer<>(o, blockReader));
            }
        }

        private static final class BlockReaderProducer<T extends Iterable<ChecksummedPositional<XIndexBlock>>> extends AtomicLong implements Producer {
            /** */
            private static final long serialVersionUID = -1L;
            private final Subscriber<? super T> o;
            private final IndexBlockReader blockReader;

            BlockReaderProducer(Subscriber<? super T> o, IndexBlockReader blockReader) {
                this.o = o;
                this.blockReader = blockReader;
            }

            @Override
            public void request(long n) {
                if (get() == MAX_VALUE) {
                    // already started with fast-path
                    return;
                }
                if (n == MAX_VALUE && compareAndSet(0, MAX_VALUE)) {
                    fastpath();
                } else if (n > 0 && getAndAddRequest(this, n) == 0L) {
                    slowpath(n, 0);
                }

            }

            void slowpath(long n, long numberEmitted) {
                if (o.isUnsubscribed()) {
                    // don't recurse
                } else if (numberEmitted < n) {
                    if (blockReader.hasNext()) {
                        blockReader.next()
                                .subscribe(new Subscriber<Iterable<ChecksummedPositional<XIndexBlock>>>() {
                                    @Override
                                    public void onCompleted() {
                                        o.onCompleted();
                                    }

                                    @Override
                                    public void onError(Throwable e) {
                                        o.onError(e);
                                    }

                                    @Override
                                    public void onNext(Iterable<ChecksummedPositional<XIndexBlock>> checksummedPositionals) {
                                        o.onNext((T) checksummedPositionals);
                                        blockReader.vertx.runOnContext(event -> slowpath(n, numberEmitted + 1));
                                    }
                                });
                    } else {
                        if (!o.isUnsubscribed()) {
                            o.onCompleted();
                        }
                    }
                } else if (numberEmitted >= n) {
                    if (!o.isUnsubscribed()) {
                        o.onCompleted();
                    }
                }
            }

            void fastpath() {
                // fast-path without backpressure
                final Subscriber<? super T> o = this.o;
                final IndexBlockReader it = this.blockReader;

                if (o.isUnsubscribed()) {
                    return;
                } else if (it.hasNext()) {
                    it.next().subscribe(
                            new Subscriber<Iterable<ChecksummedPositional<XIndexBlock>>>() {
                                @Override
                                public void onCompleted() {
                                    o.onCompleted();
                                }

                                @Override
                                public void onError(Throwable e) {
                                    o.onError(e);
                                }

                                @Override
                                public void onNext(Iterable<ChecksummedPositional<XIndexBlock>> checksummedPositionals) {
                                    o.onNext((T) checksummedPositionals);
                                    blockReader.vertx.runOnContext(event -> fastpath());
                                }
                            });
                } else if (!o.isUnsubscribed()) {
                    o.onCompleted();
                    return;
                } else {
                    // is unsubscribed
                    return;
                }
            }
        }

    }

}
