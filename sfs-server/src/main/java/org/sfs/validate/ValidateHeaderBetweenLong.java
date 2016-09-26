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
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

public class ValidateHeaderBetweenLong implements Func1<SfsRequest, SfsRequest> {


    private final String headerName;
    private final long min;
    private final long max;

    public ValidateHeaderBetweenLong(String headerName, long min, long max) {
        this.headerName = headerName;
        this.min = min;
        this.max = max;
    }

    @Override
    public SfsRequest call(SfsRequest httpServerRequest) {
        MultiMap headers = httpServerRequest.headers();

        String value = headers.get(headerName);
        if (value != null) {
            Long parsed = tryParse(value);
            if (parsed == null || parsed < min || parsed > max) {
                JsonObject jsonObject = new JsonObject()
                        .put("message", format("%s must be between %d and %d", headerName, min, max));

                throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
            }
        }
        return httpServerRequest;
    }
}