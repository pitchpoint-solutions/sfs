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

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import org.sfs.SfsRequest;
import org.sfs.util.HttpRequestValidationException;
import rx.functions.Func1;

import static com.google.common.primitives.Longs.tryParse;
import static java.lang.Long.MAX_VALUE;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AFTER;
import static org.sfs.util.SfsHttpHeaders.X_DELETE_AT;

public class ValidateTtl implements Func1<SfsRequest, SfsRequest> {


    @Override
    public SfsRequest call(SfsRequest httpServerRequest) {
        MultiMap headers = httpServerRequest.headers();

        if (headers.contains(X_DELETE_AT)
                && headers.contains(X_DELETE_AFTER)) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", format("Only one of %s or %s is allowed", X_DELETE_AFTER, X_DELETE_AT));

            throw new HttpRequestValidationException(HTTP_CONFLICT, jsonObject);
        }

        String deleteAt = headers.get(X_DELETE_AT);
        if (deleteAt != null) {
            long minDeleteAt = currentTimeMillis();
            Long parsed = tryParse(deleteAt);
            if (parsed == null || parsed < 0) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", format("%s must be between %d and %d", X_DELETE_AT, minDeleteAt, MAX_VALUE));

                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
            }
        }

        String deleteAfter = headers.get(X_DELETE_AFTER);
        if (deleteAfter != null) {
            Long parsed = tryParse(deleteAfter);
            if (parsed == null || parsed < 0) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", format("%s must be between %d and %d", X_DELETE_AFTER, 60000, MAX_VALUE));

                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
            }
        }
        return httpServerRequest;
    }

}