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

package org.sfs.math;

import org.junit.Assert;
import org.junit.Test;

public class RoundingTest {

    @Test
    public void test(){
        System.out.println(Rounding.up(136359, 8192));
    }

    @Test
    public void testUp() {
        int blockSize = 8 * 1024;
        // less
        Assert.assertEquals(blockSize, Rounding.up(0, blockSize));
        Assert.assertEquals(blockSize, Rounding.up(1, blockSize));
        Assert.assertEquals(blockSize, Rounding.up(2, blockSize));

        // equals
        Assert.assertEquals(blockSize, Rounding.up(blockSize, blockSize));

        // multiple
        Assert.assertEquals(blockSize * 2, Rounding.up(blockSize * 2, blockSize));

        // not multiple
        Assert.assertEquals(blockSize * 2, Rounding.up(blockSize + 3, blockSize));
    }

    @Test
    public void testDown() {
        int blockSize = 8 * 1024;
        // less
        Assert.assertEquals(0, Rounding.down(0, blockSize));
        Assert.assertEquals(0, Rounding.down(1, blockSize));
        Assert.assertEquals(0, Rounding.down(2, blockSize));

        // equals
        Assert.assertEquals(blockSize, Rounding.down(blockSize, blockSize));

        // multiple
        Assert.assertEquals(blockSize * 2, Rounding.down(blockSize * 2, blockSize));

        // not multiple
        Assert.assertEquals(blockSize, Rounding.down(blockSize + 3, blockSize));
    }

}