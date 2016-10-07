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

import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.hash;

public class Range {

    private final long first;
    private final long last;
    private final long blockCount;

    public Range(long first, long last) {
        checkState(last >= first, "Last must be >= first");
        this.first = first;
        this.last = last;
        this.blockCount = last - first + 1;
    }

    public long getFirst() {
        return first;
    }

    public long getLast() {
        return last;
    }

    public long getBlockCount() {
        return blockCount;
    }

    public boolean isEmpty() {
        return first == last;
    }

    @Override
    public String toString() {
        return "Range{" +
                "first=" + first +
                ", last=" + last +
                ", blockCount=" + blockCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Range)) return false;
        Range range = (Range) o;
        return Objects.equals(first, range.first) &&
                Objects.equals(last, range.last);
    }

    @Override
    public int hashCode() {
        return hash(first, last);
    }

    public boolean intersects(Range other) {
        if (other.first >= first && other.first <= last) {
            return true;
        }
        return other.last <= last && other.last >= first;
    }

    public boolean adjacent(Range other) {
        if (other.last == first - 1) {
            return true;
        }
        return other.first == last + 1;
    }

    public Range merge(Range other) {
        checkState(other.intersects(this) || other.adjacent(this), "Range does not intersect or is not adjacent to this range");
        long newFirst = other.first < first ? other.first : first;
        long newLast = other.last > last ? other.last : last;
        return new Range(newFirst, newLast);
    }

    public boolean encloses(long first, long last) {
        return this.first <= first && this.last >= last;
    }

    public boolean encloses(Range other) {
        return encloses(other.first, other.last);
    }

    public Range[] remove(Range toRemove) {
        return remove(toRemove.first, toRemove.last);
    }

    public Range[] remove(long first, long last) {
        checkState(encloses(first, last), "Range does not enclose this range");
        if (this.first == first && this.last == last) {
            return new Range[]{};
        } else if (this.first == first) {
            return new Range[]{new Range(last + 1, this.last)};
        } else if (this.last == last) {
            return new Range[]{new Range(this.first, first - 1)};
        } else {
            return new Range[]{new Range(this.first, first - 1), new Range(last + 1, this.last)};
        }
    }
}
