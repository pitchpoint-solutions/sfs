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

import com.google.common.base.Optional;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.filesystem.volume.ReadStreamBlob;
import org.sfs.filesystem.volume.Volume;
import org.sfs.io.HttpServerResponseEndableWriteStream;
import org.sfs.io.NoEndEndableWriteStream;
import org.sfs.rx.HandleServerToBusy;
import org.sfs.rx.Holder2;
import org.sfs.rx.Terminus;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateNodeIdMatchesLocalNodeId;
import org.sfs.validate.ValidateParamBetweenLong;
import org.sfs.validate.ValidateParamExists;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.SfsHttpQueryParams.LENGTH;
import static org.sfs.util.SfsHttpQueryParams.NODE;
import static org.sfs.util.SfsHttpQueryParams.OFFSET;
import static org.sfs.util.SfsHttpQueryParams.POSITION;
import static org.sfs.util.SfsHttpQueryParams.VOLUME;

public class GetBlob implements Handler<SfsRequest> {

    private static final Logger LOGGER = getLogger(GetBlob.class);

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(aVoid -> httpServerRequest)
                .map(new ValidateParamExists(VOLUME))
                .map(new ValidateParamExists(POSITION))
                .map(new ValidateParamBetweenLong(POSITION, 0, MAX_VALUE))
                .map(new ValidateParamBetweenLong(LENGTH, 0, MAX_VALUE))
                .map(new ValidateParamBetweenLong(OFFSET, 0, MAX_VALUE))
                .flatMap(httpServerRequest1 -> {

                    httpServerRequest1.response().setChunked(true);

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

                    final Volume volume = vertxContext.verticle().nodes().volumeManager().get(volumeId).get();

                    return volume.getDataStream(httpServerRequest1.vertxContext().vertx(), position, oOffset, oLength)
                            .map(readStreamBlob -> new Holder2<>(httpServerRequest1, readStreamBlob));
                })
                .map(new WriteHeaderBlobAsHttpResponseHeaders<>())
                .map(new WriteReadStreamBlobAsHttpResponseHeaders<>())
                .flatMap(input -> {
                    Optional<ReadStreamBlob> oReadStreamBlob = input.value1();
                    if (oReadStreamBlob.isPresent()) {
                        ReadStreamBlob readStreamBlob = oReadStreamBlob.get();
                        HttpServerResponse httpServerResponse = input.value0().response();
                        httpServerResponse.setStatusCode(HTTP_OK);
                        NoEndEndableWriteStream endableWriteStream = new NoEndEndableWriteStream(new HttpServerResponseEndableWriteStream(httpServerResponse));
                        return readStreamBlob.produce(endableWriteStream);
                    } else {
                        input.value0().response().setStatusCode(HTTP_NOT_FOUND);
                    }
                    return empty();
                })
                .single()
                .onErrorResumeNext(new HandleServerToBusy<>())
                .subscribe(new Terminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void aVoid) {
                        // do nothing here since we had to write the status before the stream was written
                    }
                });

    }
}