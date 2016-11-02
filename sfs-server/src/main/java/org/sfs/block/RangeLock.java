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

package org.sfs.block;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.sfs.SfsVertx;
import org.sfs.rx.Defer;
import org.sfs.rx.Holder2;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func0;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.math.LongMath.checkedAdd;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Math.floor;
import static java.lang.Math.random;
import static java.lang.System.currentTimeMillis;
import static org.sfs.math.Rounding.up;
import static rx.Observable.just;
import static rx.Observable.using;

public class RangeLock {

    private static final Logger LOGGER = getLogger(RangeLock.class);
    private final int blockSize;
    private final Object mutex = new Object();
    private final List<LockedRange> readLocks = new ArrayList<>();
    private final List<LockedRange> writeLocks = new ArrayList<>();
    private AtomicInteger lockCount = new AtomicInteger(0);

    public RangeLock(int blockSize) {
        this.blockSize = blockSize;
    }

    public static <R> Observable<R> lockedObservable(SfsVertx vertx,
                                                     Func0<Optional<Lock>> lockFactory,
                                                     Func0<? extends Observable<R>> observableFactory,
                                                     long lockWaitTimeoutMs) {

        ObservableFuture<R> handler = RxHelper.observableFuture();
        long now = currentTimeMillis();
        AtomicBoolean unSubscribed = new AtomicBoolean(false);
        lockedObservable0(vertx, now, lockFactory, observableFactory, lockWaitTimeoutMs, handler, unSubscribed);
        return handler
                .doOnUnsubscribe(() -> unSubscribed.set(true));
    }

    private static <R> void lockedObservable0(SfsVertx vertx,
                                              long startTimeMs,
                                              Func0<Optional<Lock>> lockFactory,
                                              Func0<? extends Observable<R>> observableFactory,
                                              long lockWaitTimeoutMs,
                                              ObservableFuture<R> handler,
                                              AtomicBoolean unSubscribed) {
        using(
                lockFactory::call,
                oLock -> {
                    if (oLock.isPresent()) {
                        Lock lock = oLock.get();
                        ObservableFuture<Holder2<Boolean, R>> innerHandler = RxHelper.observableFuture();
                        Defer.aVoid()
                                .flatMap(aVoid -> observableFactory.call())
                                .subscribe(new Subscriber<R>() {

                                    R value;

                                    @Override
                                    public void onCompleted() {
                                        lock.unlock();
                                        innerHandler.complete(new Holder2<>(TRUE, value));
                                    }

                                    @Override
                                    public void onError(Throwable e) {
                                        lock.unlock();
                                        innerHandler.fail(e);
                                    }

                                    @Override
                                    public void onNext(R r) {
                                        value = r;
                                    }
                                });
                        return innerHandler;
                    } else {
                        return just(new Holder2<>(FALSE, (R) null));
                    }
                },
                oLock -> {
                    if (oLock.isPresent()) {
                        oLock.get().unlock();
                    }
                })
                .subscribe(new Subscriber<Holder2<Boolean, R>>() {

                    Holder2<Boolean, ? extends R> statusHolder;

                    @Override
                    public void onCompleted() {
                        if (TRUE.equals(statusHolder.value0())) {
                            handler.complete(statusHolder.value1());
                        } else {
                            if (currentTimeMillis() - startTimeMs >= lockWaitTimeoutMs) {
                                TimedOutException timedOutException = new TimedOutException();
                                handler.fail(timedOutException);
                            } else {
                                if (!unSubscribed.get()) {
                                    vertx.setTimer((long) (floor(random() * 100) + 1), event -> lockedObservable0(vertx, startTimeMs, lockFactory, observableFactory, lockWaitTimeoutMs, handler, unSubscribed));
                                }
                            }
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        handler.fail(e);
                    }

                    @Override
                    public void onNext(Holder2<Boolean, R> status) {
                        statusHolder = status;
                    }
                });
    }

    public int getLockCount() {
        return lockCount.get();
    }

    public Optional<Lock> tryWriteLock(long position, long length) {
        return fromNullable(lockWrite0(position, length));
    }

    public Optional<Lock> tryReadLock(long position, long length) {
        return fromNullable(lockRead0(position, length));
    }

    protected Lock lockWrite0(long position, long length) {
        Range range = new Range(position, computeLast(position, length));
        synchronized (mutex) {

            if (isWriteConflict(range)
                    || isReadConflict(range)) {
                return null;
            }

            LockedRange lockedRange = new LockedRange(range);
            addWriteLockedRange(lockedRange);
            lockCount.incrementAndGet();
            return new Lock() {
                @Override
                void unlock0() {
                    synchronized (mutex) {
                        removeWriteLockedRange(lockedRange);
                        lockCount.decrementAndGet();
                    }
                }
            };
        }
    }

    protected Lock lockRead0(long position, long length) {
        Range range = new Range(position, computeLast(position, length));
        synchronized (mutex) {

            if (isWriteConflict(range)) {
                return null;
            }

            LockedRange lockedRange = new LockedRange(range);
            addReadLockedRange(lockedRange);
            lockCount.incrementAndGet();
            return new Lock() {
                @Override
                void unlock0() {
                    synchronized (mutex) {
                        removeReadLockedRange(lockedRange);
                        lockCount.decrementAndGet();
                    }
                }
            };
        }
    }

    private void removeReadLockedRange(LockedRange lockedRange) {
        checkState(readLocks.remove(lockedRange));
    }

    private void removeWriteLockedRange(LockedRange lockedRange) {
        checkState(writeLocks.remove(lockedRange));
    }

    private void addWriteLockedRange(LockedRange lockedRange) {
        checkState(writeLocks.add(lockedRange));
    }

    private void addReadLockedRange(LockedRange lockedRange) {
        checkState(readLocks.add(lockedRange));
    }

    protected boolean isReadConflict(Range range) {
        for (LockedRange lockedRange : readLocks) {
            if (lockedRange.intersects(range)) {
                return true;
            }
        }
        return false;
    }

    protected boolean isWriteConflict(Range range) {
        for (LockedRange lockedRange : writeLocks) {
            if (lockedRange.intersects(range)) {
                return true;
            }
        }
        return false;
    }

    protected long computeLast(long first, long length) {
        long last;
        try {
            last = checkedAdd(first, up(length, blockSize));
        } catch (ArithmeticException e) {
            last = MAX_VALUE;
        }
        return last - 1;
    }

    public static class LockedRange {

        private final Range range;

        public LockedRange(Range range) {
            this.range = range;
        }

        public boolean adjacent(Range other) {
            return range.adjacent(other);
        }

        public Range merge(Range other) {
            return range.merge(other);
        }

        public boolean encloses(Range other) {
            return range.encloses(other);
        }

        public Range[] remove(Range toRemove) {
            return range.remove(toRemove);
        }

        public long getLast() {
            return range.getLast();
        }

        public long getFirst() {
            return range.getFirst();
        }

        public boolean intersects(Range other) {
            return range.intersects(other);
        }

        public boolean isEmpty() {
            return range.isEmpty();
        }

        public long getBlockCount() {
            return range.getBlockCount();
        }

        public boolean encloses(long first, long last) {
            return range.encloses(first, last);
        }

        public Range[] remove(long first, long last) {
            return range.remove(first, last);
        }

        @Override
        public String toString() {
            return "LockedRange{" +
                    "range=" + range +
                    '}';
        }
    }

    public abstract static class Lock {

        private final AtomicBoolean unlocked = new AtomicBoolean(false);

        public Lock() {
        }

        public boolean unlock() {
            if (unlocked.compareAndSet(false, true)) {
                unlock0();
                return true;
            }
            return false;
        }

        abstract void unlock0();

    }

    public static class TimedOutException extends RuntimeException {

    }

}
