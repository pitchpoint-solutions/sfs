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

package org.sfs.nodes.master;


import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.rx.Terminus;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateNodeIsMasterNode;
import rx.Observable;

import static com.google.common.base.Charsets.UTF_8;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.jobs.Jobs.JobsAlreadyRunningException;
import static org.sfs.rx.Defer.empty;
import static org.sfs.rx.Defer.just;
import static org.sfs.util.ExceptionHelper.containsException;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.SfsHttpHeaders.X_SFS_KEEP_ALIVE_TIMEOUT;
import static rx.Observable.error;

public class RunNodeJobs implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(RunNodeJobs.class);

    @Override
    public void handle(SfsRequest httpServerRequest) {

        httpServerRequest.pause();

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(new ValidateNodeIsMasterNode<>(vertxContext))
                .map(aVoid -> httpServerRequest)
                .map(sfsRequest -> {
                    MultiMap headers = sfsRequest.headers();
                    final long keepAliveTimeout = headers.contains(X_SFS_KEEP_ALIVE_TIMEOUT) ? parseLong(headers.get(X_SFS_KEEP_ALIVE_TIMEOUT)) : SECONDS.toMillis(10);

                    // let the client know we're alive by sending pings on the response stream
                    httpServerRequest.startProxyKeepAlive(keepAliveTimeout, MILLISECONDS);
                    return sfsRequest;
                })
                .flatMap(sfsRequest -> vertxContext.verticle().jobs().run(vertxContext))
                .map(aVoid -> true)
                .onErrorResumeNext(throwable -> {
                    if (containsException(JobsAlreadyRunningException.class, throwable)) {
                        return just(false);
                    }
                    return error(throwable);
                })
                .flatMap(started -> httpServerRequest.stopKeepAlive()
                        .map(aVoid -> started))
                .onErrorResumeNext(throwable ->
                        httpServerRequest.stopKeepAlive()
                                .flatMap(aVoid -> Observable.<Boolean>error(throwable)))
                .single()
                .subscribe(new Terminus<Boolean>(httpServerRequest) {

                    @Override
                    public void onNext(Boolean jobsRan) {
                        JsonObject responseJson = new JsonObject()
                                .put("code", jobsRan ? HTTP_OK : HTTP_UNAVAILABLE)
                                .put("message", jobsRan ? "Success" : "Jobs already running");
                        httpServerRequest.response()
                                .write(responseJson.encode(), UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });
    }
}

