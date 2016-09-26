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

package org.sfs.util;

import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

public class HttpRequestValidationException extends RuntimeException {

    private final int statusCode;
    private final JsonObject entity;

    public HttpRequestValidationException(int statusCode, JsonObject entity) {
        super(entity.encodePrettily());
        this.statusCode = statusCode;
        this.entity = entity;
        checkEntity(this.entity);
        this.entity.put("code", statusCode);
    }

    public HttpRequestValidationException(String message, int statusCode, JsonObject entity) {
        super(message);
        this.statusCode = statusCode;
        this.entity = entity;
        checkEntity(this.entity);
        this.entity.put("code", statusCode);
    }

    public HttpRequestValidationException(String message, Throwable cause, int statusCode, JsonObject entity) {
        super(message, cause);
        this.statusCode = statusCode;
        this.entity = entity;
        checkEntity(this.entity);
        this.entity.put("code", statusCode);
    }

    public HttpRequestValidationException(Throwable cause, int statusCode, JsonObject entity) {
        super(entity.encodePrettily(), cause);
        this.statusCode = statusCode;
        this.entity = entity;
        checkEntity(this.entity);
        this.entity.put("code", statusCode);
    }

    public HttpRequestValidationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, int statusCode, JsonObject entity) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.statusCode = statusCode;
        this.entity = entity;
        checkEntity(this.entity);
        this.entity.put("code", statusCode);
    }

    private void checkEntity(JsonObject entity) {
        checkArgument(entity.getValue("code") == null, "code is a reserved field");
    }

    public int getStatusCode() {
        return statusCode;
    }

    public JsonObject getEntity() {
        return entity;
    }
}
