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

package org.sfs.block;

import com.google.common.base.Optional;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.sfs.block.RangeLock.Lock;

public class RangeLockTest {

    @Test
    public void testLockUnlock() {
        RangeLock rangeLock = new RangeLock(8192);
        Lock l = rangeLock.tryWriteLock(0, 8192 * 2).get();
        l.unlock();
    }

    @Test
    public void testFirstWriterLockSecondWriterLock() {
        RangeLock rangeLock = new RangeLock(8192);
        Optional<Lock> l = rangeLock.tryWriteLock(0, 8192 * 2);
        Optional<Lock> l2 = rangeLock.tryWriteLock(0, 8192 * 2);
        assertTrue(l.isPresent());
        assertFalse(l2.isPresent());
    }

    @Test
    public void testFirstWriterLockSecondReaderLock() {
        RangeLock rangeLock = new RangeLock(8192);
        Optional<Lock> l = rangeLock.tryWriteLock(0, 8192 * 2);
        Optional<Lock> l2 = rangeLock.tryReadLock(0, 8192 * 2);
        assertTrue(l.isPresent());
        assertFalse(l2.isPresent());
    }

    @Test
    public void testFirstReaderLockSecondReaderLock() {
        RangeLock rangeLock = new RangeLock(8192);
        Optional<Lock> l = rangeLock.tryReadLock(0, 8192 * 2);
        Optional<Lock> l2 = rangeLock.tryReadLock(0, 8192 * 2);
        assertTrue(l.isPresent());
        assertTrue(l2.isPresent());
    }


    @Test
    public void testFirstReaderLockSecondWriterLock() {
        RangeLock rangeLock = new RangeLock(8192);
        Optional<Lock> l = rangeLock.tryReadLock(0, 8192 * 2);
        Optional<Lock> l2 = rangeLock.tryWriteLock(0, 8192 * 2);
        assertTrue(l.isPresent());
        assertFalse(l2.isPresent());
    }

    @Test
    public void testLockRangeInBetween0() {
        RangeLock rangeLock = new RangeLock(8192);
        Lock l0 = rangeLock.tryWriteLock(0, 8192).get();
        Lock l1 = rangeLock.tryWriteLock(8192 * 4, 8192).get();
        assertFalse(rangeLock.tryWriteLock(8192 * 3, 8192 * 2).isPresent());
    }

    @Test
    public void testLockRangeInBetween1() {
        RangeLock rangeLock = new RangeLock(8192);
        Lock l0 = rangeLock.tryWriteLock(0, 8192).get();
        Lock l1 = rangeLock.tryWriteLock(8192 * 2, 8192).get();
        Lock l2 = rangeLock.tryWriteLock(8192, 8192).get();

    }

    @Test
    public void testLockRangeInBetween2() {
        RangeLock rangeLock = new RangeLock(8192);
        Lock l0 = rangeLock.tryWriteLock(0, 8192).get();
        Lock l1 = rangeLock.tryWriteLock(8192 * 2, 8192 * 2).get();
        assertFalse(rangeLock.tryWriteLock(8192 * 3, 8192).isPresent());

    }
}