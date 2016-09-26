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

import io.vertx.core.buffer.Buffer;
import org.junit.Assert;
import org.junit.Test;
import org.sfs.io.Block;
import org.sfs.protobuf.XVolume;

public class IndexBlockTest {

    @Test
    public void testBlockSize() {
        XVolume.XIndexBlock xHeader = XVolume.XIndexBlock.newBuilder()
                .setDataPosition(Long.MAX_VALUE)
                .setDataLength(Long.MAX_VALUE)
                .setGarbageCollected(true)
                .setDeleted(true)
                .setAcknowledged(true)
                .setUpdatedTs(Long.MAX_VALUE)
                .build();

        Buffer buffer = Block.encodeFrame(Buffer.buffer(xHeader.toByteArray())).getData();

        System.out.println("Header size is " + buffer.length());

        Assert.assertTrue(buffer.length() <= VolumeV1.INDEX_BLOCK_SIZE);
    }

}