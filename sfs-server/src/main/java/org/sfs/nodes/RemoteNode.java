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

import com.google.common.base.Optional;
import com.google.common.escape.Escaper;
import com.google.common.net.HostAndPort;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.filesystem.volume.HeaderBlob;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.io.BufferEndableWriteStream;
import org.sfs.io.EndableReadStream;
import org.sfs.io.HttpClientRequestEndableWriteStream;
import org.sfs.rx.BufferToJsonObject;
import org.sfs.rx.Defer;
import org.sfs.rx.HttpClientKeepAliveResponseBodyBuffer;
import org.sfs.rx.HttpClientResponseBodyBuffer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.util.HttpClientRequestAndResponse;
import org.sfs.util.MessageDigestFactory;
import org.sfs.util.SfsHttpHeaders;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;

import java.util.Collection;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.io.BaseEncoding.base64;
import static com.google.common.net.UrlEscapers.urlFragmentEscaper;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NOT_MODIFIED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.io.AsyncIO.pump;
import static org.sfs.rx.RxHelper.combineSinglesDelayError;
import static org.sfs.util.SfsHttpHeaders.X_SFS_REMOTE_NODE_TOKEN;
import static org.sfs.util.SfsHttpQueryParams.KEEP_ALIVE_TIMEOUT;
import static org.sfs.util.SfsHttpQueryParams.LENGTH;
import static org.sfs.util.SfsHttpQueryParams.OFFSET;
import static org.sfs.util.SfsHttpQueryParams.POSITION;
import static org.sfs.util.SfsHttpQueryParams.VOLUME;
import static org.sfs.util.SfsHttpQueryParams.X_CONTENT_COMPUTED_DIGEST_PREFIX;
import static rx.Observable.just;

public class RemoteNode extends AbstractNode {

    private static final Logger LOGGER = getLogger(RemoteNode.class);
    private final Vertx vertx;
    private final Collection<HostAndPort> hostAndPorts;
    private final int responseTimeout;
    private final String remoteNodeSecret;
    private final HttpClient httpClient;
    private final Nodes nodes;

    public RemoteNode(VertxContext<Server> vertxContext, int responseTimeout, Collection<HostAndPort> hostAndPorts) {
        this.hostAndPorts = hostAndPorts;
        this.responseTimeout = responseTimeout;
        this.remoteNodeSecret = base64().encode(vertxContext.verticle().getRemoteNodeSecret());
        this.httpClient = vertxContext.verticle().httpClient(false);
        this.nodes = vertxContext.verticle().nodes();
        this.vertx = vertxContext.vertx();
    }


    @Override
    public Observable<Optional<TransientServiceDef>> getNodeStats() {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    String url =
                                            format("http://%s/_internal_node/stats", hostAndPort.toString());

                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("getNodeStats " + url);
                                    }

                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .getAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .flatMap(httpClientResponse ->
                        Defer.just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer(HTTP_OK))
                                .map(new BufferToJsonObject())
                                .filter(entries -> !entries.isEmpty())
                                .map(jsonObject -> {
                                    TransientServiceDef transientServiceDef = new TransientServiceDef();
                                    transientServiceDef.merge(jsonObject);
                                    return transientServiceDef;
                                })
                                .map(Optional::of)
                )
                .singleOrDefault(Optional.absent());
    }


    @Override
    public Observable<Optional<DigestBlob>> checksum(String volumeId, long position, Optional<Long> oOffset, Optional<Long> oLength, MessageDigestFactory... messageDigestFactories) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    Escaper escaper = urlFragmentEscaper();

                                    StringBuilder urlBuilder =
                                            new StringBuilder("http://")
                                                    .append(hostAndPort.toString());
                                    urlBuilder = urlBuilder.append("/_internal_node_data/blob/checksum?");
                                    urlBuilder = urlBuilder.append(KEEP_ALIVE_TIMEOUT);
                                    urlBuilder = urlBuilder.append('=');
                                    urlBuilder = urlBuilder.append(responseTimeout / 2);
                                    urlBuilder = urlBuilder.append('&');
                                    urlBuilder = urlBuilder.append(VOLUME);
                                    urlBuilder = urlBuilder.append('=');
                                    urlBuilder = urlBuilder.append(escaper.escape(volumeId));
                                    urlBuilder = urlBuilder.append('&');
                                    urlBuilder = urlBuilder.append(escaper.escape(POSITION));
                                    urlBuilder = urlBuilder.append('=');
                                    urlBuilder = urlBuilder.append(position);

                                    if (messageDigestFactories.length > 0) {
                                        for (MessageDigestFactory instance : messageDigestFactories) {
                                            urlBuilder = urlBuilder.append('&');
                                            urlBuilder =
                                                    urlBuilder
                                                            .append(escaper.escape(format("%s%s", X_CONTENT_COMPUTED_DIGEST_PREFIX, instance.getValue())))
                                                            .append('=')
                                                            .append("true");
                                        }
                                    }

                                    final String url = urlBuilder.toString();

                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("get " + url);
                                    }

                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .getAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .flatMap(httpClientResponse ->
                        just(httpClientResponse)
                                .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                                .map(buffer -> {
                                    if (HTTP_OK != httpClientResponse.statusCode()) {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("Buffer is " + buffer.toString());
                                    }
                                    return buffer;
                                })
                                .map(new BufferToJsonObject())
                                .map(jsonObject -> {
                                    Integer code = jsonObject.getInteger("code");
                                    if (code != null) {
                                        if (HTTP_OK == code) {
                                            return of(jsonObject);
                                        } else if (HTTP_NOT_FOUND == code) {
                                            return Optional.<JsonObject>absent();
                                        }
                                    }
                                    throw new HttpClientResponseException(httpClientResponse, jsonObject);
                                })
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .map(jsonObject -> {
                                    JsonObject blob = jsonObject.getJsonObject("blob");
                                    return of(new DigestBlob(blob));
                                }))
                .singleOrDefault(absent());
    }

    @Override
    public Observable<Optional<HeaderBlob>> delete(String volumeId, final long position) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    Escaper escaper = urlFragmentEscaper();

                                    String url =
                                            format("http://%s/_internal_node_data/blob?%s=%s&%s=%d",
                                                    hostAndPort.toString(),
                                                    escaper.escape(VOLUME),
                                                    escaper.escape(volumeId),
                                                    escaper.escape(POSITION),
                                                    position);

                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("delete " + url);
                                    }

                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .deleteAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .flatMap(httpClientResponse ->
                        Defer.just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer())
                                .map(buffer -> {
                                    int status = httpClientResponse.statusCode();
                                    if (HTTP_NOT_MODIFIED == status) {
                                        return absent();

                                    } else if (HTTP_NO_CONTENT == status) {
                                        return of(new HeaderBlob(httpClientResponse));

                                    } else {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }

                                }));
    }

    @Override
    public Observable<Optional<HeaderBlob>> acknowledge(String volumeId, final long position) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    Escaper escaper = urlFragmentEscaper();
                                    final String url =
                                            format("http://%s/_internal_node_data/blob/ack?%s=%s&%s=%d",
                                                    hostAndPort.toString(),
                                                    escaper.escape(VOLUME),
                                                    escaper.escape(volumeId),
                                                    escaper.escape(POSITION),
                                                    position);

                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("acknowledge " + url);
                                    }

                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .putAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .flatMap(httpClientResponse ->
                        Defer.just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer())
                                .map(buffer -> {
                                    int status = httpClientResponse.statusCode();
                                    if (HTTP_NOT_MODIFIED == status) {
                                        return absent();

                                    } else if (HTTP_NO_CONTENT == status) {
                                        return of(new HeaderBlob(httpClientResponse));

                                    } else {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }
                                }));

    }

    @Override
    public Observable<Optional<ReadStreamBlob>> createReadStream(String volumeId, final long position, final Optional<Long> oOffset, final Optional<Long> oLength) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    Escaper escaper = urlFragmentEscaper();

                                    StringBuilder urlBuilder =
                                            new StringBuilder("http://")
                                                    .append(hostAndPort.toString())
                                                    .append("/_internal_node_data/blob")
                                                    .append('?')
                                                    .append(escaper.escape(VOLUME))
                                                    .append('=')
                                                    .append(escaper.escape(volumeId))
                                                    .append('&')
                                                    .append(escaper.escape(POSITION))
                                                    .append('=')
                                                    .append(position);

                                    if (oOffset.isPresent()) {
                                        long offset = oOffset.get();
                                        urlBuilder =
                                                urlBuilder.append('&')
                                                        .append(escaper.escape(OFFSET))
                                                        .append('=')
                                                        .append(offset);
                                    }

                                    if (oLength.isPresent()) {
                                        long length = oLength.get();
                                        urlBuilder =
                                                urlBuilder.append('&')
                                                        .append(escaper.escape(LENGTH))
                                                        .append('=')
                                                        .append(length);
                                    }

                                    String url = urlBuilder.toString();

                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("createReadStream " + url);
                                    }

                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .getAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .flatMap(httpClientResponse -> {

                    int status = httpClientResponse.statusCode();
                    if (HTTP_OK == status) {
                        ReadStreamBlob readStreamBlob = new ReadStreamBlob(httpClientResponse) {
                            @Override
                            public Observable<Void> produce(BufferEndableWriteStream endableWriteStream) {
                                return pump(EndableReadStream.from(httpClientResponse), endableWriteStream);
                            }
                        };
                        return just(of(readStreamBlob));
                    } else if (HTTP_NOT_FOUND == status) {
                        return just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer())
                                .map(buffer -> absent());
                    } else {
                        return just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer())
                                .map(buffer -> {
                                    throw new HttpClientResponseException(httpClientResponse, buffer);
                                });
                    }
                });
    }


    @Override
    public Observable<Boolean> canReadVolume(String volumeId) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    Escaper escaper = urlFragmentEscaper();

                                    String url = format("http://%s/_internal_node_data/blob/canread?%s=%s",
                                            hostAndPort.toString(),
                                            escaper.escape(VOLUME),
                                            escaper.escape(escaper.escape(volumeId)));


                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient.getAbs(url,
                                                    httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .flatMap(httpClientResponse ->
                        just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer())
                                .map(buffer -> {
                                    int status = httpClientResponse.statusCode();
                                    if (status >= 400) {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }
                                    return true;
                                }));
    }

    @Override
    public Observable<Boolean> canWriteVolume(String volumeId) {
        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    Escaper escaper = urlFragmentEscaper();

                                    String url = format("http://%s/_internal_node_data/blob/canwrite?%s=%s",
                                            hostAndPort.toString(),
                                            escaper.escape(VOLUME),
                                            escaper.escape(escaper.escape(volumeId)));


                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient.getAbs(url,
                                                    httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout);
                                    httpClientRequest.end();

                                    return handler.map(httpClientResponse -> new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse));
                                }))
                .map(HttpClientRequestAndResponse::getResponse)
                .flatMap(httpClientResponse ->
                        just(httpClientResponse)
                                .flatMap(new HttpClientResponseBodyBuffer())
                                .map(buffer -> {
                                    int status = httpClientResponse.statusCode();
                                    if (status >= 400) {
                                        throw new HttpClientResponseException(httpClientResponse, buffer);
                                    }
                                    return true;
                                }));
    }

    @Override
    public Observable<NodeWriteStreamBlob> createWriteStream(final String volumeId, final long length, final MessageDigestFactory... messageDigestFactories) {
        final XNode _this = this;

        return Defer.aVoid()
                .flatMap(aVoid ->
                        nodes.connectFirstAvailable(
                                vertx,
                                hostAndPorts,
                                hostAndPort -> {
                                    Escaper escaper = urlFragmentEscaper();

                                    StringBuilder urlBuilder =
                                            new StringBuilder("http://")
                                                    .append(hostAndPort.toString());
                                    urlBuilder = urlBuilder.append("/_internal_node_data/blob?");
                                    urlBuilder = urlBuilder.append(KEEP_ALIVE_TIMEOUT);
                                    urlBuilder = urlBuilder.append('=');
                                    urlBuilder = urlBuilder.append(responseTimeout / 2);
                                    urlBuilder = urlBuilder.append('&');
                                    urlBuilder = urlBuilder.append(VOLUME);
                                    urlBuilder = urlBuilder.append('=');
                                    urlBuilder = urlBuilder.append(escaper.escape(volumeId));

                                    if (messageDigestFactories.length > 0) {
                                        for (MessageDigestFactory instance : messageDigestFactories) {
                                            urlBuilder = urlBuilder.append('&');
                                            urlBuilder =
                                                    urlBuilder
                                                            .append(escaper.escape(format("%s%s", X_CONTENT_COMPUTED_DIGEST_PREFIX, instance.getValue())))
                                                            .append('=')
                                                            .append("true");
                                        }
                                    }

                                    final String url = urlBuilder.toString();

                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("createWriteStream " + url);
                                    }

                                    ObservableFuture<HttpClientResponse> handler = RxHelper.observableFuture();

                                    HttpClientRequest httpClientRequest =
                                            httpClient
                                                    .putAbs(url, httpClientResponse -> {
                                                        httpClientResponse.pause();
                                                        handler.complete(httpClientResponse);
                                                    })
                                                    .exceptionHandler(handler::fail)
                                                    .putHeader(SfsHttpHeaders.X_CONTENT_LENGTH, String.valueOf(length))
                                                    .putHeader(X_SFS_REMOTE_NODE_TOKEN, remoteNodeSecret)
                                                    .setTimeout(responseTimeout)
                                                    .setChunked(true);
                                    httpClientRequest.sendHead();

                                    return handler.map(httpClientResponse -> {
                                        return new HttpClientRequestAndResponse(httpClientRequest, httpClientResponse);
                                    });
                                }))
                .map(httpClientRequestAndResponse -> {
                    HttpClientRequest httpClientRequest = httpClientRequestAndResponse.getRequest();
                    HttpClientResponse httpClientResponse = httpClientRequestAndResponse.getResponse();

                    if (HTTP_OK != httpClientResponse.statusCode()) {
                        httpClientResponse.resume();
                        throw new HttpClientResponseException(httpClientResponse, Buffer.buffer());
                    }
                    Observable<DigestBlob> oResponse =
                            Defer.just(httpClientResponse)
                                    .flatMap(new HttpClientKeepAliveResponseBodyBuffer())
                                    .map(new BufferToJsonObject())
                                    .map(jsonObject -> {
                                        Integer code = jsonObject.getInteger("code");
                                        if (code == null || HTTP_OK != code) {
                                            throw new HttpClientResponseException(httpClientResponse, jsonObject);
                                        }
                                        return jsonObject;
                                    })
                                    .map(jsonObject -> {
                                        JsonObject blob = jsonObject.getJsonObject("blob");
                                        return new DigestBlob(blob);
                                    });

                    NodeWriteStreamBlob writeStreamBlob = new NodeWriteStreamBlob(_this) {
                        @Override
                        public Observable<DigestBlob> consume(EndableReadStream<Buffer> src) {

                            return combineSinglesDelayError(
                                    pump(src, new HttpClientRequestEndableWriteStream(httpClientRequest)),
                                    oResponse,
                                    (aVoid1, digestBlob) -> digestBlob);


                        }
                    };
                    return writeStreamBlob;
                });
    }

    @Override
    public String toString() {
        return "RemoteNode{" +
                "hostAndPorts=" + hostAndPorts +
                '}';
    }
}
