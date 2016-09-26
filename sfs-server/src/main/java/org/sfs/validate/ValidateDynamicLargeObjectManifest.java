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

package org.sfs.validate;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.vo.TransientVersion;
import rx.functions.Func1;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.sfs.util.SfsHttpHeaders.X_OBJECT_MANIFEST;
import static org.sfs.vo.ObjectPath.fromPaths;

public class ValidateDynamicLargeObjectManifest implements Func1<TransientVersion, TransientVersion> {

    public ValidateDynamicLargeObjectManifest() {
    }

    @Override
    public TransientVersion call(TransientVersion transientVersion) {
        Optional<String> oObjectManifest = transientVersion.getObjectManifest();
        if (oObjectManifest.isPresent()) {
            String actualContainerId = fromPaths(transientVersion.getParent().getParent().getParent().getId(), oObjectManifest.get()).containerPath().get();
            String expectedContainerId = transientVersion.getParent().getParent().getId();
            if (!expectedContainerId.equals(actualContainerId)) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", format("%s can only reference object in the container %s", X_OBJECT_MANIFEST, expectedContainerId));

                throw new HttpRequestValidationException(HTTP_NOT_FOUND, jsonObject);
            }
        }

        return transientVersion;
    }
}