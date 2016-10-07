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

package org.sfs.nodes.all.masterkey;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.encryption.MasterKeyCheckEndableWriteStream;
import org.sfs.encryption.MasterKeyCheckStreamProducer;
import org.sfs.rx.Terminus;
import org.sfs.validate.ValidateActionAdminOrSystem;
import rx.Observable;

import static com.google.common.base.Charsets.UTF_8;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.SfsHttpHeaders.X_SFS_KEEP_ALIVE_TIMEOUT;

public class MasterKeysCheck implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(MasterKeysCheck.class);

    @Override
    public void handle(SfsRequest httpServerRequest) {

        httpServerRequest.pause();

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .flatMap(aVoid -> {

                    MultiMap headers = httpServerRequest.headers();

                    final long keepAliveTimeout = headers.contains(X_SFS_KEEP_ALIVE_TIMEOUT) ? parseLong(headers.get(X_SFS_KEEP_ALIVE_TIMEOUT)) : SECONDS.toMillis(10);

                    // let the client know we're alive by sending pings on the response stream
                    httpServerRequest.startProxyKeepAlive(keepAliveTimeout, MILLISECONDS);

                    MasterKeyCheckStreamProducer producer = new MasterKeyCheckStreamProducer(vertxContext);
                    MasterKeyCheckEndableWriteStream consumer = new MasterKeyCheckEndableWriteStream(vertxContext);

                    return pump(producer, consumer);

                })
                .flatMap(aVoid -> httpServerRequest.stopKeepAlive())
                .onErrorResumeNext(throwable ->
                        httpServerRequest.stopKeepAlive()
                                .flatMap(aVoid -> Observable.<Void>error(throwable)))
                .single()
                .subscribe(new Terminus<Void>(httpServerRequest) {

                    @Override
                    public void onNext(Void aVoid) {
                        JsonObject responseJson = new JsonObject()
                                .put("code", HTTP_OK)
                                .put("message", "Success");
                        httpServerRequest.response()
                                .write(responseJson.encode(), UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });
    }
}

