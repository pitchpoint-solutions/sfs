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

package org.sfs.nodes.data;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.nodes.LocalNode;
import org.sfs.rx.HandleServerToBusy;
import org.sfs.rx.Holder2;
import org.sfs.rx.Terminus;
import org.sfs.util.MessageDigestFactory;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateNodeIdMatchesLocalNodeId;
import org.sfs.validate.ValidateParamBetweenLong;
import org.sfs.validate.ValidateParamComputedDigest;
import org.sfs.validate.ValidateParamExists;
import rx.Observable;

import java.util.regex.Matcher;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.toArray;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.MessageDigestFactory.fromValueIfExists;
import static org.sfs.util.SfsHttpQueryParams.COMPUTED_DIGEST;
import static org.sfs.util.SfsHttpQueryParams.KEEP_ALIVE_TIMEOUT;
import static org.sfs.util.SfsHttpQueryParams.LENGTH;
import static org.sfs.util.SfsHttpQueryParams.NODE;
import static org.sfs.util.SfsHttpQueryParams.OFFSET;
import static org.sfs.util.SfsHttpQueryParams.POSITION;
import static org.sfs.util.SfsHttpQueryParams.VOLUME;

public class ChecksumBlob implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateParamExists(NODE))
                .map(new ValidateParamExists(VOLUME))
                .map(new ValidateParamExists(POSITION))
                .map(new ValidateParamBetweenLong(POSITION, 0, MAX_VALUE))
                .map(new ValidateParamBetweenLong(LENGTH, 0, MAX_VALUE))
                .map(new ValidateParamBetweenLong(OFFSET, 0, MAX_VALUE))
                .map(new ValidateNodeIdMatchesLocalNodeId<>(vertxContext, httpServerRequest.params().get(NODE)))
                .map(new ValidateParamComputedDigest())
                .flatMap(httpServerRequest1 -> {
                    MultiMap params = httpServerRequest1.params();

                    final Iterable<MessageDigestFactory> iterable =
                            from(params.names())
                                    .transform(new Function<String, Optional<MessageDigestFactory>>() {
                                        @Override
                                        public Optional<MessageDigestFactory> apply(String param) {
                                            Matcher matcher = COMPUTED_DIGEST.matcher(param);
                                            if (matcher.matches()) {
                                                String digestName = matcher.group(1);
                                                return fromValueIfExists(digestName);
                                            }
                                            return absent();
                                        }
                                    })
                                    .filter(Optional::isPresent)
                                    .transform(input -> input.get());

                    MultiMap queryParams = httpServerRequest1.params();

                    String volumeId = queryParams.get(VOLUME);
                    long position = parseLong(queryParams.get(POSITION));

                    Optional<Long> oOffset;
                    if (queryParams.contains(OFFSET)) {
                        oOffset = of(parseLong(queryParams.get(OFFSET)));
                    } else {
                        oOffset = absent();
                    }

                    Optional<Long> oLength;
                    if (queryParams.contains(LENGTH)) {
                        oLength = of(parseLong(queryParams.get(LENGTH)));
                    } else {
                        oLength = absent();
                    }

                    final long keepAliveTimeout = parseLong(params.get(KEEP_ALIVE_TIMEOUT));

                    // let the client know we're alive by sending pings on the response stream
                    httpServerRequest1.startProxyKeepAlive(keepAliveTimeout, MILLISECONDS);

                    LocalNode localNode = new LocalNode(vertxContext, vertxContext.verticle().nodes().volumeManager());

                    return localNode.checksum(volumeId, position, oOffset, oLength, toArray(iterable, MessageDigestFactory.class))
                            .map(digestBlobOptional -> new Holder2<>(httpServerRequest1, digestBlobOptional));
                })
                .flatMap(holder -> httpServerRequest.stopKeepAlive()
                        .map(aVoid -> holder))
                .onErrorResumeNext(throwable ->
                        httpServerRequest.stopKeepAlive()
                                .flatMap(aVoid -> Observable.<Holder2<SfsRequest, Optional<DigestBlob>>>error(throwable)))
                .single()
                .onErrorResumeNext(new HandleServerToBusy<>())
                .subscribe(new Terminus<Holder2<SfsRequest, Optional<DigestBlob>>>(httpServerRequest) {
                    @Override
                    public void onNext(Holder2<SfsRequest, Optional<DigestBlob>> holder) {
                        Optional<DigestBlob> oJsonDigestBlob = holder.value1();
                        JsonObject jsonResponse = new JsonObject();
                        if (oJsonDigestBlob.isPresent()) {
                            jsonResponse.put("code", HTTP_OK)
                                    .put("blob", oJsonDigestBlob.get().toJsonObject());
                        } else {
                            jsonResponse.put("code", HTTP_NOT_FOUND);
                        }
                        HttpServerResponse httpResponse = holder.value0().response();
                        httpResponse.write(jsonResponse.encode(), UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });

    }
}