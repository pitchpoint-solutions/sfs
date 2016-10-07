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

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import org.sfs.SfsVertx;
import org.sfs.io.BufferReadStream;
import org.sfs.io.CountingReadStream;
import rx.Observable;
import rx.functions.Func1;

import static org.sfs.util.VertxAssert.assertEquals;

public class PutFile<T1> implements Func1<T1, Observable<Long>> {

    private final TestContext context;
    private final SfsVertx vertx;
    private final Volume volume;
    private final Buffer expectedBuffer;
    private final long expectedWritePosition;

    public PutFile(TestContext context, SfsVertx vertx, Volume volume, Buffer expectedBuffer, long expectedWritePosition) {
        this.context = context;
        this.vertx = vertx;
        this.volume = volume;
        this.expectedBuffer = expectedBuffer;
        this.expectedWritePosition = expectedWritePosition;
    }

    @Override
    public Observable<Long> call(T1 aVoid) {
        return volume.putDataStream(vertx, expectedBuffer.length())
                .flatMap(new Func1<WriteStreamBlob, Observable<Long>>() {
                    @Override
                    public Observable<Long> call(final WriteStreamBlob writeStreamBlob) {
                        BufferReadStream bufferReadStream = new BufferReadStream(expectedBuffer);
                        CountingReadStream countingReadStream = new CountingReadStream(bufferReadStream);
                        return writeStreamBlob.consume(countingReadStream)
                                .map(new Func1<Void, Void>() {
                                    @Override
                                    public Void call(Void aVoid) {
                                        assertEquals(context, expectedBuffer.length(), countingReadStream.count());
                                        assertEquals(context, expectedWritePosition, writeStreamBlob.getPosition());
                                        return null;
                                    }
                                })
                                .map(new Func1<Void, Long>() {
                                    @Override
                                    public Long call(Void aVoid) {
                                        return expectedWritePosition;
                                    }
                                });
                    }
                });
    }
}
