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

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import org.sfs.math.Rounding;

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

public class RecyclingAllocator {

    private final Object mutex = new Object();
    private AtomicLong numberOfFreeRanges = new AtomicLong(0);
    private final NavigableMap<Long, Range> byPosition = new ConcurrentSkipListMap<>();
    private final NavigableMap<Long, NavigableSet<Range>> bySize = new ConcurrentSkipListMap<>();
    private final Comparator<Range> byPositionComparator = (o1, o2) -> Long.compare(o1.getFirst(), o2.getFirst());
    private final int blockSize;

    public RecyclingAllocator(int blockSize) {
        Preconditions.checkArgument(blockSize >= 1, "BlockSize must be >= 1");
        this.blockSize = blockSize;
        Range toMerge = new Range(0, computeLast(0, Rounding.down(Long.MAX_VALUE, blockSize)));
        free0(toMerge);
    }

    public long allocNextAvailable(long length) {
        Range match = null;
        synchronized (mutex) {
            Map.Entry<Long, NavigableSet<Range>> entry = bySize.ceilingEntry(length);
            if (entry != null) {
                long numberOfBlocks = entry.getKey();
                NavigableSet<Range> sortedByPosition = entry.getValue();
                Preconditions.checkNotNull(sortedByPosition);
                match = sortedByPosition.pollFirst();
                Preconditions.checkNotNull(match);
                if (sortedByPosition.isEmpty()) {
                    Preconditions.checkState(sortedByPosition == bySize.remove(numberOfBlocks));
                }
                Preconditions.checkState(match == byPosition.remove(match.getFirst()));
                numberOfFreeRanges.decrementAndGet();
            }
            Preconditions.checkNotNull(match);
            long position = match.getFirst();
            Range[] updated = match.remove(position, computeLast(position, length));
            for (Range update : updated) {
                putRange(update);
            }
            return position;
        }
    }

    public long alloc(long position, long length) {
        checkRange(position, length);
        Range toRemove = new Range(position, computeLast(position, length));
        return alloc0(toRemove);
    }

    protected long alloc0(Range toRemove) {
        long position = toRemove.getFirst();
        Range match = null;
        synchronized (mutex) {
            Map.Entry<Long, Range> floorEntry = byPosition.floorEntry(position);
            if (floorEntry != null) {
                Range range = floorEntry.getValue();
                if (range.encloses(toRemove)) {
                    match = range;
                    removeRange(range);
                }
            }
            if (match != null) {
                Range[] updated = match.remove(toRemove);
                for (Range update : updated) {
                    putRange(update);
                }
                return position;
            } else {
                return -1;
            }
        }
    }

    private void removeRange(Range range) {
        long blockCount = range.getBlockCount();
        NavigableSet<Range> sortedByPosition = bySize.get(blockCount);
        Preconditions.checkNotNull(sortedByPosition);
        Preconditions.checkState(sortedByPosition.remove(range));
        if (sortedByPosition.isEmpty()) {
            Preconditions.checkState(sortedByPosition == bySize.remove(blockCount));
        }
        Preconditions.checkState(range == byPosition.remove(range.getFirst()));
        numberOfFreeRanges.decrementAndGet();
    }

    private void putRange(Range range) {
        long blockCount = range.getBlockCount();
        NavigableSet<Range> sortedBySize = bySize.get(blockCount);
        if (sortedBySize == null) {
            sortedBySize = newSortedByPosition();
            Preconditions.checkState(bySize.put(blockCount, sortedBySize) == null);
        }
        Preconditions.checkState(sortedBySize.add(range));
        Preconditions.checkState(byPosition.put(range.getFirst(), range) == null);
        numberOfFreeRanges.incrementAndGet();
    }

    private NavigableSet<Range> newSortedByPosition() {
        return new ConcurrentSkipListSet<>(byPositionComparator);
    }


    public void free(long position, long length) {
        checkRange(position, length);
        Range toMerge = new Range(position, computeLast(position, length));
        free0(toMerge);
    }

    protected void free0(Range toMerge) {
        long first = toMerge.getFirst();
        synchronized (mutex) {
            Map.Entry<Long, Range> floorEntry = byPosition.floorEntry(first);
            Map.Entry<Long, Range> ceilingEntry = byPosition.ceilingEntry(first);

            if (floorEntry != null) {
                Range floorRange = floorEntry.getValue();
                if (floorRange.intersects(toMerge) || floorRange.adjacent(toMerge)) {
                    removeRange(floorRange);
                    toMerge = floorRange.merge(toMerge);
                }
            }

            if (ceilingEntry != null) {
                Range ceilingRange = ceilingEntry.getValue();
                if (ceilingRange.intersects(toMerge) || ceilingRange.adjacent(toMerge)) {
                    removeRange(ceilingRange);
                    toMerge = ceilingRange.merge(toMerge);
                }
            }

            putRange(toMerge);
        }
    }

    protected void checkRange(long position, long length) {
        Preconditions.checkArgument(position % blockSize == 0, "Position is not a multiple of the block size, Was %s", position);
        Preconditions.checkArgument(position >= 0, "Position must be >= 0, Was %s", position);
        Preconditions.checkArgument(length >= 0, "Length must be >= 0, Was %s", length);
    }

    public Iterable<Range> freeRanges() {
        return byPosition.values();
    }

    public long greatestFreePosition() {
        return byPosition.lastEntry().getKey();
    }

    public int getNumberOfFreeRanges() {
        long num = numberOfFreeRanges.get();
        if (num > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) num;
    }

    public long getBytesFree(long useableSpace) {
        synchronized (mutex) {
            useableSpace = Math.max(0, useableSpace);
            long free = 0;
            Range last = byPosition.lastEntry().getValue();
            for (Range range : byPosition.values()) {
                if (range == last) {
                    long firstPosition = range.getFirst();
                    long length = Rounding.down(useableSpace, blockSize);
                    long lastPosition = computeLast(firstPosition, length - firstPosition);
                    Range adjustedLast = new Range(firstPosition, lastPosition);
                    free = LongMath.checkedAdd(free, adjustedLast.getBlockCount());
                } else {
                    free = LongMath.checkedAdd(free, range.getBlockCount());
                }
            }
            return free;
        }
    }

    protected long computeLast(long first, long length) {
        return LongMath.checkedAdd(first, Rounding.up(length, blockSize)) - 1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Range freeRange : freeRanges()) {
            sb.append("Range: ");
            sb.append(freeRange);
            sb.append('\n');
        }
        return sb.toString();
    }
}
