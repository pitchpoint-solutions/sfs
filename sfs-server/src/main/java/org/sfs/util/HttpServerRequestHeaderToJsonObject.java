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

package org.sfs.util;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;

import java.util.List;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class HttpServerRequestHeaderToJsonObject {

    private static final Logger LOGGER = getLogger(HttpServerRequestHeaderToJsonObject.class);

    public static JsonObject call(HttpServerRequest httpServerRequest) {
        JsonObject jsonObject = new JsonObject();

        String query = httpServerRequest.query();
        String requestLine = String.format("%s %s%s %s", httpServerRequest.method(), httpServerRequest.path(), query != null ? '?' + query : "", httpServerRequest.version().toString());
        jsonObject.put("request_line", requestLine);
        MultiMap headers = httpServerRequest.headers();
        JsonArray jsonHeaders = new JsonArray();
        for (String headerName : headers.names()) {
            List<String> values = headers.getAll(headerName);
            JsonObject jsonHeader = new JsonObject();
            jsonHeader.put(headerName, values);
            jsonHeaders.add(jsonHeader);
        }
        jsonObject.put("headers", jsonHeaders);
        return jsonObject;
    }
}
