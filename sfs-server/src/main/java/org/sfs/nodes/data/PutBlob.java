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
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.filesystem.volume.DigestBlob;
import org.sfs.filesystem.volume.Volume;
import org.sfs.filesystem.volume.VolumeManager;
import org.sfs.io.CountingReadStream;
import org.sfs.io.DigestReadStream;
import org.sfs.rx.HandleServerToBusy;
import org.sfs.rx.Holder2;
import org.sfs.rx.Terminus;
import org.sfs.util.MessageDigestFactory;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateHeaderBetweenLong;
import org.sfs.validate.ValidateHeaderExists;
import org.sfs.validate.ValidateNodeIdMatchesLocalNodeId;
import org.sfs.validate.ValidateParamComputedDigest;
import org.sfs.validate.ValidateParamExists;
import rx.Observable;

import java.util.regex.Matcher;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.KeepAliveHttpServerResponse.DELIMITER_BUFFER;
import static org.sfs.util.MessageDigestFactory.fromValueIfExists;
import static org.sfs.util.SfsHttpQueryParams.COMPUTED_DIGEST;
import static org.sfs.util.SfsHttpQueryParams.KEEP_ALIVE_TIMEOUT;
import static org.sfs.util.SfsHttpQueryParams.NODE;
import static org.sfs.util.SfsHttpQueryParams.VOLUME;

public class PutBlob implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(PutBlob.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        httpServerRequest.pause();

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateParamExists(NODE))
                .map(new ValidateParamExists(VOLUME))
                .map(new ValidateHeaderExists(CONTENT_LENGTH))
                .map(new ValidateHeaderBetweenLong(CONTENT_LENGTH, 0, MAX_VALUE))
                .map(new ValidateNodeIdMatchesLocalNodeId<>(vertxContext, httpServerRequest.params().get(NODE)))
                .map(new ValidateParamComputedDigest())
                .flatMap(httpServerRequest1 -> {

                    MultiMap headers = httpServerRequest1.headers();
                    MultiMap params = httpServerRequest1.params();

                    String volumeId = params.get(VOLUME);

                    final Iterable<MessageDigestFactory> iterable =
                            from(params.names())
                                    .transform(new Function<String, Optional<MessageDigestFactory>>() {
                                        @Override
                                        public Optional<MessageDigestFactory> apply(String param) {
                                            Matcher matcher = COMPUTED_DIGEST.matcher(param);
                                            if (matcher.find()) {
                                                String digestName = matcher.group(1);
                                                return fromValueIfExists(digestName);
                                            }
                                            return absent();
                                        }
                                    })
                                    .filter(Optional::isPresent)
                                    .transform(input -> input.get());

                    VolumeManager volumeManager = vertxContext.verticle().nodes().volumeManager();

                    final Volume volume = volumeManager.get(volumeId).get();
                    final long length = parseLong(headers.get(CONTENT_LENGTH));

                    final long keepAliveTimeout = parseLong(params.get(KEEP_ALIVE_TIMEOUT));

                    // let the client know we're alive by sending pings on the response stream
                    httpServerRequest1.startProxyKeepAlive(keepAliveTimeout, MILLISECONDS);

                    return volume.putDataStream(httpServerRequest1.vertxContext().vertx(), length)
                            .flatMap(writeStreamBlob -> {

                                DigestReadStream digestReadStream = new DigestReadStream(httpServerRequest1, toArray(iterable, MessageDigestFactory.class));
                                CountingReadStream countingReadStream = new CountingReadStream(digestReadStream);
                                return writeStreamBlob.consume(countingReadStream)
                                        .map(aVoid -> {
                                            DigestBlob digestBlob = new DigestBlob(writeStreamBlob.getVolume(), writeStreamBlob.isPrimary(), writeStreamBlob.isReplica(), writeStreamBlob.getPosition(), countingReadStream.count());
                                            for (Holder2<MessageDigestFactory, byte[]> digest : digestReadStream.digests()) {
                                                digestBlob.withDigest(digest.value0(), digest.value1());
                                            }
                                            return new Holder2<>(httpServerRequest1, of(digestBlob));
                                        });
                            });
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
                            jsonResponse.put("code", HTTP_INTERNAL_ERROR);
                        }
                        HttpServerResponse httpResponse = holder.value0().response();
                        httpResponse.write(jsonResponse.encode(), UTF_8.toString())
                                .write(DELIMITER_BUFFER);
                    }
                });

    }
}
