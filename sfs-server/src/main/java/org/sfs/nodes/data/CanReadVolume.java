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

package org.sfs.nodes.data;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.nodes.LocalNode;
import org.sfs.rx.Terminus;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateNodeIsDataNode;
import org.sfs.validate.ValidateParamExists;

import static java.lang.Boolean.TRUE;
import static java.net.HttpURLConnection.HTTP_NOT_ACCEPTABLE;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.util.SfsHttpQueryParams.VOLUME;

public class CanReadVolume implements Handler<SfsRequest> {

    @Override
    public void handle(SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(new ValidateNodeIsDataNode<>(vertxContext))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateParamExists(VOLUME))
                .flatMap(httpServerRequest1 -> {

                    MultiMap params = httpServerRequest1.params();

                    String volumeId = params.get(VOLUME);

                    VolumeManager volumeManager = vertxContext.verticle().nodes().volumeManager();

                    LocalNode localNode = new LocalNode(vertxContext, volumeManager);

                    return localNode.canReadVolume(volumeId);
                })
                .single()
                .subscribe(new Terminus<Boolean>(httpServerRequest) {
                    @Override
                    public void onNext(Boolean canContinue) {
                        if (TRUE.equals(canContinue)) {
                            httpServerRequest.response().setStatusCode(HTTP_OK);
                        } else {
                            httpServerRequest.response().setStatusCode(HTTP_NOT_ACCEPTABLE);
                        }
                    }
                });
    }
}
