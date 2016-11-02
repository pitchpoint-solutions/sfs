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

package org.sfs.jobs;

import com.google.common.base.Strings;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import org.sfs.util.HttpRequestValidationException;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

public class JobParams {

    public static String getFirstRequiredParam(MultiMap params, String name) {
        String value = params.get(name);
        if (Strings.isNullOrEmpty(value)) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", String.format("%s is required", name));

            throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
        } else {
            return value;
        }
    }
}
