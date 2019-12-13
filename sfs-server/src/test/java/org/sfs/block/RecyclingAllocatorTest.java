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

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sfs.TestSubscriber;
import org.sfs.math.Rounding;

import static com.google.common.collect.Iterables.toArray;
import static java.lang.System.out;
import static org.sfs.util.VertxAssert.assertArrayEquals;
import static org.sfs.util.VertxAssert.assertEquals;
import static rx.Observable.just;

@RunWith(VertxUnitRunner.class)
public class RecyclingAllocatorTest {

    @Test
    public void testGetBytesFree(TestContext context) {
        int blockSize = 8 * 1024;
        RecyclingAllocator allocator = new RecyclingAllocator(blockSize);

        Async async = context.async();
        just((Void) null)
                .map(aVoid -> {
                    allocator.allocNextAvailable(1);
                    long position = allocator.allocNextAvailable(1);
                    allocator.allocNextAvailable(1);
                    allocator.free(position, 1);
                    int size = 100000;
                    long roundedSize = Rounding.down(size, blockSize);
                    long free = allocator.getBytesFree(size);
                    assertEquals(context, roundedSize - (blockSize * 2), free);
                    assertEquals(context, 2, allocator.getNumberOfFreeRanges());
                    return (Void) null;
                })
                .map(aVoid -> {
                    int size = 0;
                    long free = allocator.getBytesFree(size);
                    assertEquals(context, (blockSize * 2), free);
                    return (Void) null;
                })
                .map(aVoid -> {
                    int size = -10;
                    long free = allocator.getBytesFree(size);
                    assertEquals(context, (blockSize * 2), free);
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testAllocate(TestContext context) {
        int blockSize = 8 * 1024;
        RecyclingAllocator allocator = new RecyclingAllocator(blockSize);

        Async async = context.async();
        just((Void) null)
                .map(aVoid -> allocator.allocNextAvailable(2))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, 0L, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(8192, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(100))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(16384, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(100))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 2, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(24576, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));

    }

    @Test
    public void testRecycle(TestContext context) {
        int blockSize = 8 * 1024;
        RecyclingAllocator allocator = new RecyclingAllocator(blockSize);
        long bytesFree = allocator.getBytesFree(100000);

        Async async = context.async();
        just((Void) null)
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, 0, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - blockSize, allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize * 2, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 2), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 2, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize * 3, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 3), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(blockSize * 2 + 1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 3, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize * 6, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 6), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 6, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize * 7, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 7), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> {
                    allocator.free(blockSize, 1);
                    return null;
                })
                .map(position -> {
                    print(allocator);
                    assertEquals(context, 2, allocator.getNumberOfFreeRanges());
                    Assert.assertArrayEquals(
                            new Range[]{
                                    new Range(blockSize, 16383),
                                    new Range(blockSize * 7, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 6), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(2))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize * 7, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 7), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 7, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize * 8, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 8), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 8, position.longValue());
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(blockSize * 9, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    assertEquals(context, bytesFree - (blockSize * 9), allocator.getBytesFree(100000));
                    return (Void) null;
                })
                .map(aVoid -> allocator.greatestFreePosition())
                .map(position -> {
                    assertEquals(context, 73728L, position.longValue());
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testAllocSpecific(TestContext context) {
        int blockSize = 8 * 1024;
        RecyclingAllocator allocator = new RecyclingAllocator(blockSize);

        Async async = context.async();
        just((Void) null)
                .map(aVoid -> allocator.alloc(blockSize, 2))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize, position.longValue());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(0, blockSize - 1),
                                    new Range(blockSize * 2, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .map(aVoid -> allocator.alloc(blockSize * 10000, 2))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 10000, position.longValue());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(0, blockSize - 1),
                                    new Range(blockSize * 2, 81919999),
                                    new Range(81928192, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .map(aVoid -> allocator.alloc(blockSize, 2))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, -1, position.longValue());
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testAllocRandomPositions(TestContext context) {

        int blockSize = 8 * 1024;
        RecyclingAllocator allocator = new RecyclingAllocator(blockSize);

        Async async = context.async();
        just((Void) null)
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 1, position.longValue());
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 2, position.longValue());
                    return (Void) null;
                })
                .map(aVoid -> allocator.allocNextAvailable(1))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 3, position.longValue());
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));

    }

    @Test
    public void testAllocRandomFree(TestContext context) {

        int blockSize = 8 * 1024;
        RecyclingAllocator allocator = new RecyclingAllocator(blockSize);

        Async async = context.async();
        just((Void) null)
                .map(aVoid -> allocator.allocNextAvailable(100000000))
                .map(position -> {
                    print(allocator);
                    assertEquals(context, 1, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{new Range(100007936, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .map(aVoid -> {
                    allocator.free(0, 10);
                    return null;
                })
                .map(position -> {
                    print(allocator);
                    assertEquals(context, 2, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(0, 8191),
                                    new Range(100007936, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .map(aVoid -> {
                    allocator.free(blockSize * 3, 10);
                    return null;
                })
                .map(position -> {
                    print(allocator);
                    assertEquals(context, 3, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(0, 8191),
                                    new Range(24576, 32767),
                                    new Range(100007936, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .map(aVoid -> {
                    allocator.free(blockSize, blockSize * 3);
                    return null;
                })
                .map(position -> {
                    print(allocator);
                    assertEquals(context, 2, allocator.getNumberOfFreeRanges());
                    assertArrayEquals(
                            context,
                            new Range[]{
                                    new Range(0, 32767),
                                    new Range(100007936, 9223372036854767615L)},
                            toArray(allocator.freeRanges(), Range.class));
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }


    @Test
    public void testLastAllocated(TestContext context) {
        int blockSize = 8 * 1024;
        RecyclingAllocator allocator = new RecyclingAllocator(blockSize);

        Async async = context.async();
        just((Void) null)
                .map(aVoid -> allocator.allocNextAvailable(blockSize * 3))
                .map(aVoid -> allocator.greatestFreePosition())
                .map(position -> {
                    print(allocator);
                    assertEquals(context, blockSize * 3, position.longValue());
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }


    protected void print(RecyclingAllocator allocator) {
        for (Range freeRange : allocator.freeRanges()) {
            out.println("Range: " + freeRange);
        }
        out.println();
    }
}