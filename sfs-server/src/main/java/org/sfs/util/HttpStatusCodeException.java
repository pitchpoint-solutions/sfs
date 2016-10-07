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

public class HttpStatusCodeException extends RuntimeException {

    private final int statusCode;

    public HttpStatusCodeException(int statusCode) {
        this.statusCode = statusCode;
    }

    public HttpStatusCodeException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public HttpStatusCodeException(String message, Throwable cause, int statusCode) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public HttpStatusCodeException(Throwable cause, int statusCode) {
        super(cause);
        this.statusCode = statusCode;
    }

    public HttpStatusCodeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, int statusCode) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
