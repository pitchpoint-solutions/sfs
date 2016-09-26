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

import com.google.common.base.Optional;
import io.vertx.ext.unit.TestContext;
import org.sfs.SfsVertx;
import rx.Observable;
import rx.functions.Func1;

import static org.sfs.util.VertxAssert.assertTrue;

public class DeleteFile implements Func1<Long, Observable<Long>> {

    private final TestContext testContext;
    private final SfsVertx vertx;
    private final Volume volume;

    public DeleteFile(TestContext testContext, SfsVertx vertx, Volume volume) {
        this.testContext = testContext;
        this.vertx = vertx;
        this.volume = volume;
    }


    @Override
    public Observable<Long> call(final Long position) {
        return volume.delete(vertx, position)
                .map(new Func1<Optional<HeaderBlob>, Long>() {
                    @Override
                    public Long call(Optional<HeaderBlob> headerBlobOptional) {
                        assertTrue(testContext, headerBlobOptional.isPresent());
                        return position;
                    }
                });
    }

}
