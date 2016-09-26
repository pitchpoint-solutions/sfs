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

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RangeTest {

    @Test
    public void testMergeIntersect0() {
        Range rangeOne = new Range(0, 10);
        Range rangeTwo = new Range(5, 15);
        assertTrue(rangeOne.intersects(rangeTwo));
        assertFalse(rangeOne.adjacent(rangeTwo));
        assertEquals(new Range(0, 15), rangeOne.merge(rangeTwo));
    }

    @Test
    public void testMergeIntersect1() {
        Range rangeOne = new Range(5, 15);
        Range rangeTwo = new Range(0, 10);
        assertTrue(rangeOne.intersects(rangeTwo));
        assertFalse(rangeOne.adjacent(rangeTwo));
        assertEquals(new Range(0, 15), rangeOne.merge(rangeTwo));
    }

    @Test
    public void testMergeIntersect2() {
        Range rangeOne = new Range(5, 15);
        Range rangeTwo = new Range(5, 15);
        assertTrue(rangeOne.intersects(rangeTwo));
        assertFalse(rangeOne.adjacent(rangeTwo));
        assertEquals(new Range(5, 15), rangeOne.merge(rangeTwo));
    }

    @Test
    public void testMergeIntersect3() {
        Range rangeOne = new Range(0, 10);
        Range rangeTwo = new Range(10, 15);
        assertTrue(rangeOne.intersects(rangeTwo));
        assertFalse(rangeOne.adjacent(rangeTwo));
        assertEquals(new Range(0, 15), rangeOne.merge(rangeTwo));
    }

    @Test
    public void testMergeIntersect4() {
        Range rangeOne = new Range(5, 15);
        Range rangeTwo = new Range(0, 5);
        assertTrue(rangeOne.intersects(rangeTwo));
        assertFalse(rangeOne.adjacent(rangeTwo));
        assertEquals(new Range(0, 15), rangeOne.merge(rangeTwo));
    }

    @Test
    public void testMergeIntersect5() {
        Range rangeOne = new Range(7, 15);
        Range rangeTwo = new Range(0, 5);
        assertFalse(rangeOne.intersects(rangeTwo));
        assertFalse(rangeOne.adjacent(rangeTwo));
        try {
            assertEquals(new Range(0, 15), rangeOne.merge(rangeTwo));
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void testRemove0() {
        Range rangeOne = new Range(6, 15);
        Range[] rangeTwo = rangeOne.remove(6, 6);
        assertArrayEquals(new Range[]{new Range(7, 15)}, rangeTwo);
    }


    @Test
    public void testRemove1() {
        Range rangeOne = new Range(5, 15);
        Range[] rangeTwo = rangeOne.remove(5, 15);
        assertArrayEquals(new Range[]{}, rangeTwo);
    }

    @Test
    public void testRemove2() {
        Range rangeOne = new Range(0, 100);
        Range[] rangeTwo = rangeOne.remove(5, 15);
        assertArrayEquals(new Range[]{new Range(0, 4), new Range(16, 100)}, rangeTwo);
    }

    @Test
    public void testRemove3() {
        Range rangeOne = new Range(0, 100);
        try {
            Range[] rangeTwo = rangeOne.remove(5, 101);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void testRemove4() {
        Range rangeOne = new Range(0, 100);
        try {
            Range[] rangeTwo = rangeOne.remove(-1, 5);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void testRemove5() {
        Range rangeOne = new Range(0, 100);
        try {
            Range[] rangeTwo = rangeOne.remove(-1, 101);
            fail();
        } catch (Throwable e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void testMergeAdjacent8() {
        Range rangeOne = new Range(0, 10);
        Range rangeTwo = new Range(11, 15);
        assertFalse(rangeOne.intersects(rangeTwo));
        assertTrue(rangeOne.adjacent(rangeTwo));
        assertEquals(new Range(0, 15), rangeOne.merge(rangeTwo));
    }

    @Test
    public void testAdjacent9() {
        Range rangeOne = new Range(11, 15);
        Range rangeTwo = new Range(0, 10);
        assertFalse(rangeOne.intersects(rangeTwo));
        assertTrue(rangeOne.adjacent(rangeTwo));
        assertEquals(new Range(0, 15), rangeOne.merge(rangeTwo));
    }
}