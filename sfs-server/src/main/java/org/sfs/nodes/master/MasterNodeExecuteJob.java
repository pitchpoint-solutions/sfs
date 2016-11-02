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

package org.sfs.nodes.master;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.jobs.Jobs;
import org.sfs.rx.Defer;
import org.sfs.rx.NullSubscriber;
import org.sfs.rx.Terminus;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateHeaderBetweenLong;
import org.sfs.validate.ValidateHeaderExists;
import org.sfs.validate.ValidateNodeIsMasterNode;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.jobs.Jobs.Parameters.JOB_ID;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;

public class MasterNodeExecuteJob implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(MasterNodeExecuteJob.class);

    @Override
    public void handle(SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        Defer.aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(new ValidateNodeIsMasterNode<>(vertxContext))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateHeaderExists(Jobs.Parameters.JOB_ID))
                .map(new ValidateHeaderBetweenLong(Jobs.Parameters.TIMEOUT, 100, Long.MAX_VALUE))
                .map(new ToVoid<>())
                .flatMap(aVoid -> {

                    MultiMap headers = httpServerRequest.headers();
                    String jobId = headers.get(JOB_ID);

                    long timeout = headers.contains(Jobs.Parameters.TIMEOUT) ? Long.parseLong(headers.get(Jobs.Parameters.TIMEOUT)) : -1;

                    httpServerRequest.startProxyKeepAlive();

                    Jobs jobs = vertxContext.verticle().jobs();
                    return Defer.aVoid()
                            .doOnNext(aVoid1 ->
                                    jobs.execute(vertxContext, jobId, headers)
                                            .subscribe(new NullSubscriber<>()))
                            .flatMap(aVoid1 -> {
                                if (timeout >= 0) {
                                    return jobs.waitStopped(vertxContext, jobId, timeout, TimeUnit.MILLISECONDS);
                                } else {
                                    return Defer.aVoid();
                                }
                            });
                })
                .single()
                .subscribe(new Terminus<Void>(httpServerRequest) {

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