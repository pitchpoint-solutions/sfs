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
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import org.sfs.SfsVertx;
import org.sfs.io.CountingEndableWriteStream;
import org.sfs.io.NullEndableWriteStream;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static org.sfs.util.VertxAssert.assertEquals;

public class FileSize implements Func1<Long, Observable<Long>> {
    private final TestContext context;
    private final SfsVertx vertx;
    private final Volume volume;
    private final Buffer expectedBuffer;

    public FileSize(TestContext context, SfsVertx vertx, Volume volume, Buffer expectedBuffer) {
        this.context = context;
        this.vertx = vertx;
        this.volume = volume;
        this.expectedBuffer = expectedBuffer;
    }

    @Override
    public Observable<Long> call(final Long position) {
        return volume.getDataStream(vertx, position, absent(), absent())
                .map(new Func1<Optional<ReadStreamBlob>, ReadStreamBlob>() {
                    @Override
                    public ReadStreamBlob call(Optional<ReadStreamBlob> readStreamBlobOptional) {
                        return readStreamBlobOptional.get();
                    }
                })
                .flatMap(new Func1<ReadStreamBlob, Observable<Void>>() {
                    @Override
                    public Observable<Void> call(ReadStreamBlob readStreamBlob) {
                        final CountingEndableWriteStream countingWriteStream = new CountingEndableWriteStream(new NullEndableWriteStream());
                        return readStreamBlob.produce(countingWriteStream)
                                .map(new Func1<Void, Void>() {
                                    @Override
                                    public Void call(Void aVoid) {
                                        long expectedLength = expectedBuffer.getBytes().length;
                                        assertEquals(context, expectedLength, countingWriteStream.count());
                                        return null;
                                    }
                                });
                    }
                })
                .map(new Func1<Void, Long>() {
                    @Override
                    public Long call(Void aVoid) {
                        return position;
                    }
                });
    }

}
