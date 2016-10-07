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

import com.google.common.base.Optional;
import io.vertx.ext.unit.TestContext;
import org.sfs.SfsVertx;
import org.sfs.filesystem.ChecksummedPositional;
import rx.Observable;
import rx.functions.Func1;

import static org.sfs.protobuf.XVolume.XIndexBlock;
import static org.sfs.util.VertxAssert.assertEquals;

public class AssertHeader implements Func1<Long, Observable<Long>> {

    private final TestContext context;
    private final SfsVertx vertx;
    private final VolumeV1 sfsDataV1;
    private final XIndexBlock expectedHeader;

    public AssertHeader(TestContext context, SfsVertx vertx, VolumeV1 sfsDataV1, XIndexBlock xHeader) {
        this.context = context;
        this.vertx = vertx;
        this.sfsDataV1 = sfsDataV1;
        this.expectedHeader = xHeader;
    }

    @Override
    public Observable<Long> call(final Long position) {
        return sfsDataV1.getIndexBlock0(vertx, position)
                .map(new Func1<Optional<ChecksummedPositional<XIndexBlock>>, Long>() {
                    @Override
                    public Long call(Optional<ChecksummedPositional<XIndexBlock>> positional) {
                        XIndexBlock xHeader = positional.get().getValue();
                        assertEquals(context, expectedHeader.getAcknowledged(), xHeader.getAcknowledged());
                        assertEquals(context, expectedHeader.getDeleted(), xHeader.getDeleted());
                        assertEquals(context, expectedHeader.getDataPosition(), xHeader.getDataPosition());
                        assertEquals(context, expectedHeader.getDataLength(), xHeader.getDataLength());
                        assertEquals(context, expectedHeader.getGarbageCollected(), xHeader.getGarbageCollected());
                        return position;
                    }
                });
    }

}
