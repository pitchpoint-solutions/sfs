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

package org.sfs.nodes.compute.container;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.jobs.Jobs;
import org.sfs.nodes.ClusterInfo;
import org.sfs.nodes.MasterNode;
import org.sfs.nodes.Nodes;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.Defer;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateContainerPath;
import org.sfs.validate.ValidateHeaderBetweenLong;
import org.sfs.vo.ObjectPath;
import org.sfs.vo.TransientServiceDef;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;

public class VerifyRepairContainerExecute implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        Defer.aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateHeaderBetweenLong(Jobs.Parameters.TIMEOUT, 100, Long.MAX_VALUE))
                .map(new ToVoid<>())
                .map(aVoid -> ObjectPath.fromSfsRequest(httpServerRequest))
                .map(new ValidateContainerPath())
                .flatMap(objectPath -> {
                    ClusterInfo clusterInfo = vertxContext.verticle().getClusterInfo();
                    Nodes nodes = vertxContext.verticle().nodes();
                    MultiMap headers = httpServerRequest.headers();

                    long timeout = headers.contains(Jobs.Parameters.TIMEOUT) ? Long.parseLong(headers.get(Jobs.Parameters.TIMEOUT)) : 100;

                    String unparsedForceRemoveVolumes = headers.contains(Jobs.Parameters.FORCE_REMOVE_VOLUMES) ? headers.get(Jobs.Parameters.FORCE_REMOVE_VOLUMES) : null;

                    MultiMap params = MultiMap.caseInsensitiveMultiMap();
                    if (unparsedForceRemoveVolumes != null) {
                        params.add(Jobs.Parameters.FORCE_REMOVE_VOLUMES, unparsedForceRemoveVolumes);
                    }
                    params.set(Jobs.Parameters.CONTAINER_ID, objectPath.containerPath().get());

                    TransientServiceDef transientServiceDef = clusterInfo.getCurrentMasterNode();
                    MasterNode masterNode = nodes.remoteMasterNode(vertxContext, transientServiceDef);

                    httpServerRequest.startProxyKeepAlive();

                    return masterNode.executeJob(Jobs.ID.VERIFY_REPAIR_CONTAINER_OBJECTS, params, timeout, TimeUnit.MILLISECONDS);
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void aVoid) {
                        JsonObject responseJson = new JsonObject()
                                .put("code", HTTP_OK)
                                .put("message", "Success");
                        httpServerRequest.response()
                                .write(responseJson.encode(), StandardCharsets.UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });
    }
}