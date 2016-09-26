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

package org.sfs.nodes.compute.object;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.object.LoadAccountAndContainerAndObject;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.validate.ValidateActionAdmin;
import org.sfs.validate.ValidateObjectPath;
import org.sfs.vo.PersistentObject;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static java.lang.String.valueOf;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.rx.Defer.empty;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class GetObjectMeta implements Handler<SfsRequest> {

    @Override
    public void handle(SfsRequest httpServerRequest) {
        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdmin(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateObjectPath())
                .flatMap(new LoadAccountAndContainerAndObject(vertxContext))
                .map(PersistentObject::toJsonObject)
                .subscribe(new ConnectionCloseTerminus<JsonObject>(httpServerRequest) {
                    @Override
                    public void onNext(JsonObject input) {
                        Buffer buffer = buffer(input.encodePrettily(), UTF_8.toString());
                        httpServerRequest.response()
                                .setStatusCode(HTTP_OK)
                                .putHeader(CONTENT_LENGTH, valueOf(buffer.length()))
                                .putHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                                .write(buffer);
                    }
                });
    }
}
