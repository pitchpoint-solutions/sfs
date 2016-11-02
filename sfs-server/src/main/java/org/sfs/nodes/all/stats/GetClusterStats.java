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

package org.sfs.nodes.all.stats;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.nodes.ClusterInfo;
import org.sfs.rx.Terminus;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.vo.TransientServiceDef;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.buffer.Buffer.buffer;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.valueOf;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.rx.Defer.aVoid;

public class GetClusterStats implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(GetClusterStats.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ToVoid<>())
                .map(aVoid -> {
                    ClusterInfo clusterInfo = vertxContext.verticle().getClusterInfo();
                    JsonArray jsonArray = new JsonArray();
                    for (TransientServiceDef node : clusterInfo.getAllNodes()) {
                        jsonArray.add(node.toJsonObject());
                    }
                    return jsonArray;
                })
                .single()
                .subscribe(new Terminus<JsonArray>(httpServerRequest) {

                    @Override
                    public void onNext(JsonArray jsonArray) {
                        Buffer encoded = buffer(jsonArray.encode().getBytes(UTF_8));
                        httpServerRequest.response()
                                .setStatusCode(HTTP_OK)
                                .putHeader(CONTENT_LENGTH, valueOf(encoded.length()))
                                .write(encoded);
                    }
                });


    }

}
