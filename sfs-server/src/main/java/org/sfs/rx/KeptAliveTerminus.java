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

import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.SfsRequest;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.util.HttpStatusCodeException;
import rx.Subscriber;

import static com.google.common.base.Charsets.UTF_8;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

public abstract class KeptAliveTerminus extends Subscriber<SfsRequest> {

    private static final Logger LOGGER = getLogger(KeptAliveTerminus.class);
    private final SfsRequest httpServerRequest;

    public KeptAliveTerminus(SfsRequest httpServerRequest) {
        this.httpServerRequest = httpServerRequest;
    }

    @Override
    public void onCompleted() {
        httpServerRequest.response().end();
    }

    @Override
    public void onError(Throwable e) {
        JsonObject document = new JsonObject();
        HttpServerResponse response = httpServerRequest.response();
        if (e instanceof HttpRequestValidationException) {
            HttpRequestValidationException cause = (HttpRequestValidationException) e;
            JsonObject entity = cause.getEntity();
            int status = cause.getStatusCode();

            LOGGER.debug("Validate Error", e);
            document.put("status", status)
                    .put("message", entity.encode());

        } else if (e instanceof HttpStatusCodeException) {
            HttpStatusCodeException cause = (HttpStatusCodeException) e;

            LOGGER.debug("HttpStatusCode Error", e);
            document.put("status", cause.getStatusCode());

        } else {
            LOGGER.error("Unhandled Exception", e);
            document.put("status", HTTP_INTERNAL_ERROR);
        }
        response.end(document.encode(), UTF_8.toString());
    }

}
