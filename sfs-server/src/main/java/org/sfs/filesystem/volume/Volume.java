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
import org.sfs.SfsVertx;
import org.sfs.vo.TransientXVolume;
import rx.Observable;

import java.nio.file.Path;


public interface Volume {

    enum Status {
        STARTING,
        STOPPING,
        STARTED,
        STOPPED;

        public static Status fromNameIfExists(String name) {
            if (name != null) {
                for (Status s : values()) {
                    if (s.name().equals(name)) {
                        return s;
                    }
                }
            }
            return null;
        }
    }

    Observable<TransientXVolume> volumeInfo(SfsVertx vertx);

    Observable<Void> open(SfsVertx vertx);

    Status status();

    Observable<Void> close(SfsVertx vertx);

    String getVolumeId();

    Observable<Void> copy(SfsVertx vertx, Path destinationDirectory);

    Observable<Optional<ReadStreamBlob>> getDataStream(SfsVertx vertx, long position, Optional<Long> offset, Optional<Long> length);

    Observable<WriteStreamBlob> putDataStream(SfsVertx vertx, long length);

    Observable<Optional<HeaderBlob>> acknowledge(SfsVertx vertx, long position);

    Observable<Optional<HeaderBlob>> delete(SfsVertx vertx, final long position);
}
