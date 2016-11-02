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

package org.sfs.nodes;

import com.google.common.net.HostAndPort;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpHeaders;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.jobs.Jobs;
import org.sfs.rx.BufferToJsonObject;
import org.sfs.rx.Defer;
import org.sfs.rx.HttpClientKeepAliveResponseBodyBuffer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpBodyLogger;
import org.sfs.util.HttpClientRequestAndResponse;
import org.sfs.util.HttpClientResponseHeaderLogger;
import rx.Observable;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.BaseEncoding.base64;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.jobs.Jobs.Parameters.JOB_ID;
import static org.sfs.util.SfsHttpHeaders.X_SFS_REMOTE_NODE_TOKEN;

public class RemoteMasterNode implements MasterNode {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteMasterNode.class);
    private final Vertx vertx;
    private final Collection<HostAndPort> hostAndPorts;
    private final int responseTimeout;
    private final String remoteNodeSecret;
    private final HttpClient httpClient;
    private final Nodes nodes;

    public RemoteMasterNode(VertxContext<Server> vertxContext, int responseTimeout, Collection<HostAndPort> hostAndPorts) {
        this.responseTimeout = responseTimeout;
        this.hostAndPorts = hostAndPorts;
        this.remoteNodeSecret = base64().encode(vertxContext.verticle().getRemoteNodeSecret());
        this.httpClient = vertxContext.verticle().httpClient(false);
        this.vertx = vertxContext.vertx();
        this.nodes = vertxContext.verticle().nodes();
    }


    @Override
    public Observable<Void> executeJob(String jobId, MultiMap params, long timeout, TimeUnit timeUnit) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    String url =
                                            String.format("http://%s/_internal_node_master_execute_job/", hostAndPort.toString());

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .postAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .putHeader(JOB_ID, jobId)
                                                    .putHeader(HttpHeaders.TIMEOUT, String.valueOf(timeUnit.toMillis(timeout)))
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.headers().addAll(params);

                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(httpClientResponse ->
                        Defer.just(httpClientResponse)
                                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                                .map(new HttpBodyLogger())
                                .map(buffer -> {
                                    if (HTTP_OK != httpClientResponse.statusCode()) {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }
                                    return buffer;
                                })
                                .map(new BufferToJsonObject())
                                .map(jsonObject -> {
                                    Integer code = jsonObject.getInteger("code");
                                    if (code != null) {
                                        if (HTTP_OK == code) {
                                            return jsonObject;
                                        } else if (HTTP_CONFLICT == code) {
                                            return new Jobs.JobAlreadyRunning();
                                        } else if (HTTP_NOT_FOUND == code) {
                                            throw new Jobs.JobNotFound();
                                        }
                                    }
                                    throw new HttpClientResponseException(httpClientResponse, jsonObject);
                                }))
                .map(new ToVoid<>());
    }

    @Override
    public Observable<Void> waitForJob(String jobId, long timeout, TimeUnit timeUnit) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    String url =
                                            String.format("http://%s/_internal_node_master_wait_for_job/", hostAndPort.toString());

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .postAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .putHeader(JOB_ID, jobId)
                                                    .putHeader(HttpHeaders.TIMEOUT, String.valueOf(timeUnit.toMillis(timeout)))
                                                    .setTimeout(responseTimeout);

                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(httpClientResponse ->
                        Defer.just(httpClientResponse)
                                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                                .map(new HttpBodyLogger())
                                .map(buffer -> {
                                    if (HTTP_OK != httpClientResponse.statusCode()) {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }
                                    return buffer;
                                })
                                .map(new BufferToJsonObject())
                                .map(jsonObject -> {
                                    Integer code = jsonObject.getInteger("code");
                                    if (code != null) {
                                        if (HTTP_OK == code) {
                                            return jsonObject;
                                        } else if (HTTP_CONFLICT == code) {
                                            return new Jobs.WaitStoppedExpired();
                                        } else if (HTTP_NOT_FOUND == code) {
                                            throw new Jobs.JobNotFound();
                                        }
                                    }
                                    throw new HttpClientResponseException(httpClientResponse, jsonObject);
                                }))
                .map(new ToVoid<>());
    }

    @Override
    public Observable<Void> stopJob(String jobId, long timeout, TimeUnit timeUnit) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    String url =
                                            String.format("http://%s/_internal_node_master_stop_job/", hostAndPort.toString());

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .postAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .putHeader(JOB_ID, jobId)
                                                    .putHeader(HttpHeaders.TIMEOUT, String.valueOf(timeUnit.toMillis(timeout)))
                                                    .setTimeout(responseTimeout);

                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .map(new HttpClientResponseHeaderLogger())
                .flatMap(httpClientResponse ->
                        Defer.just(httpClientResponse)
                                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                                .map(new HttpBodyLogger())
                                .map(buffer -> {
                                    if (HTTP_OK != httpClientResponse.statusCode()) {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }
                                    return buffer;
                                })
                                .map(new BufferToJsonObject())
                                .map(jsonObject -> {
                                    Integer code = jsonObject.getInteger("code");
                                    if (code != null) {
                                        if (HTTP_OK == code) {
                                            return jsonObject;
                                        } else if (HTTP_CONFLICT == code) {
                                            return new Jobs.WaitStoppedExpired();
                                        } else if (HTTP_NOT_FOUND == code) {
                                            throw new Jobs.JobNotFound();
                                        }
                                    }
                                    throw new HttpClientResponseException(httpClientResponse, jsonObject);
                                }))
                .map(new ToVoid<>());
    }

}
