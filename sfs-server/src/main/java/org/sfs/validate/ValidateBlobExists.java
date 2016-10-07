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

package org.sfs.validate;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import org.sfs.filesystem.volume.HeaderBlob;
import org.sfs.filesystem.volume.Volume;
import org.sfs.util.HttpRequestValidationException;
import rx.functions.Func1;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class ValidateBlobExists<A extends HeaderBlob> implements Func1<Optional<A>, A> {

    private final Volume volume;
    private final long position;

    public ValidateBlobExists(Volume volume, long position) {
        this.volume = volume;
        this.position = position;
    }

    public Volume getVolume() {
        return volume;
    }

    public long getPosition() {
        return position;
    }

    @Override
    public A call(Optional<A> optional) {
        if (!optional.isPresent()) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", format("Not found in volume %s @ position %d", volume.getVolumeId(), position));

            throw new HttpRequestValidationException(HTTP_NOT_FOUND, jsonObject);
        }
        return optional.get();
    }
}
