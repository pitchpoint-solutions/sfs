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

package org.sfs.rx;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.SfsRequest;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.util.HttpStatusCodeException;
import rx.Subscriber;

import java.util.Objects;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.http.HttpMethod.HEAD;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.sfs.util.ExceptionHelper.containsException;
import static org.sfs.util.ExceptionHelper.unwrapCause;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.SfsHttpHeaders.X_AUTH_TOKEN;

public abstract class Terminus<T> extends Subscriber<T> {

    private static final Logger LOGGER = getLogger(Terminus.class);
    private final SfsRequest httpServerRequest;

    public Terminus(SfsRequest httpServerRequest) {
        this.httpServerRequest = httpServerRequest;
    }

    public SfsRequest getSfsRequest() {
        return httpServerRequest;
    }

    @Override
    public void onCompleted() {

        LOGGER.debug("Ended onComplete");
        try {
            HttpServerResponse response = httpServerRequest.response();
            response.end();
        } finally {
            httpServerRequest.resume();
        }

    }

    @Override
    public void onError(Throwable e) {
        try {
            HttpServerResponse response = httpServerRequest.response();
            response.setChunked(true);
            if (containsException(HttpRequestValidationException.class, e)) {
                HttpRequestValidationException cause = unwrapCause(HttpRequestValidationException.class, e).get();
                JsonObject entity = cause.getEntity();
                int status = cause.getStatusCode();

                // if credentials weren't supplied send unauthorized instead
                // of forbidden
                if (status == HTTP_FORBIDDEN
                        && !httpServerRequest.headers().contains(AUTHORIZATION)
                        && !httpServerRequest.headers().contains(X_AUTH_TOKEN)) {
                    status = HTTP_UNAUTHORIZED;
                    if (!httpServerRequest.proxyKeepAliveStarted()) {
                        response.putHeader(WWW_AUTHENTICATE, "Basic realm=\"Helix\"");
                    }
                }

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Validate Error " + httpServerRequest.path(), e);
                }

                HttpMethod method = httpServerRequest.method();
                if (Objects.equals(HEAD, method)) {
                    if (httpServerRequest.proxyKeepAliveStarted()) {
                        Buffer encoded = buffer(entity.encodePrettily(), UTF_8.toString())
                                .appendBuffer(DELIMITER_BUFFER);
                        response.end(encoded);
                    } else {
                        Buffer encoded = buffer(entity.encodePrettily(), UTF_8.toString());
                        response.setStatusCode(status)
                                .write(encoded)
                                .end();
                    }
                } else {
                    if (httpServerRequest.proxyKeepAliveStarted()) {
                        Buffer encoded = buffer(entity.encodePrettily(), UTF_8.toString())
                                .appendBuffer(DELIMITER_BUFFER);
                        response.end(encoded);
                    } else {
                        Buffer buffer = buffer(entity.encodePrettily(), UTF_8.toString());
                        response.setStatusCode(status)
                                .write(buffer)
                                .end(buffer);
                    }
                }

            } else if (containsException(HttpStatusCodeException.class, e)) {
                HttpStatusCodeException cause = unwrapCause(HttpStatusCodeException.class, e).get();

                int status = cause.getStatusCode();

                // if credentials weren't supplied send unauthorized instead
                // of forbidden
                if (status == HTTP_FORBIDDEN
                        && !httpServerRequest.headers().contains(AUTHORIZATION)
                        && !httpServerRequest.headers().contains(X_AUTH_TOKEN)) {
                    status = HTTP_UNAUTHORIZED;
                    if (!httpServerRequest.proxyKeepAliveStarted()) {
                        response.putHeader(WWW_AUTHENTICATE, "Basic realm=\"Helix\"");
                    }
                }

                LOGGER.error("HttpStatusCode Error " + httpServerRequest.path(), e);

                if (httpServerRequest.proxyKeepAliveStarted()) {
                    JsonObject jsonObject = new JsonObject()
                            .put("code", status);
                    Buffer encoded = buffer(jsonObject.encodePrettily(), UTF_8.toString())
                            .appendBuffer(DELIMITER_BUFFER);
                    response.end(encoded);
                } else {
                    response.setStatusCode(status)
                            .end();
                }

            } else {
                LOGGER.error("Unhandled Exception " + httpServerRequest.path(), e);
                if (httpServerRequest.proxyKeepAliveStarted()) {
                    JsonObject jsonObject = new JsonObject()
                            .put("code", HTTP_INTERNAL_ERROR);
                    Buffer encoded = buffer(jsonObject.encode(), UTF_8.toString())
                            .appendBuffer(DELIMITER_BUFFER);
                    response.end(encoded);
                } else {
                    response.setStatusCode(HTTP_INTERNAL_ERROR)
                            .end();
                }

            }
        } finally {
            httpServerRequest.resume();
        }
    }
}
