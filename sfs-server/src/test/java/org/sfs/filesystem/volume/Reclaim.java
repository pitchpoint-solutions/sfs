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

import org.sfs.SfsVertx;
import rx.Observable;
import rx.functions.Func1;

public class Reclaim implements Func1<Long, Observable<Long>> {

    private final SfsVertx vertx;
    private final VolumeV1 sfsDataV1;

    public Reclaim(SfsVertx vertx, VolumeV1 sfsDataV1) {
        this.vertx = vertx;
        this.sfsDataV1 = sfsDataV1;
    }

    @Override
    public Observable<Long> call(final Long position) {
        return sfsDataV1.garbageCollection(vertx)
                .map(new Func1<Void, Long>() {
                    @Override
                    public Long call(Void aVoid) {
                        return position;
                    }
                });
    }

}
