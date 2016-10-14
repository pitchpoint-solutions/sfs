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
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.filesystem.volume.HeaderBlob;
import org.sfs.filesystem.volume.Volume;
import org.sfs.rx.HandleServerToBusy;
import org.sfs.rx.Holder2;
import org.sfs.rx.Terminus;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateNodeIdMatchesLocalNodeId;
import org.sfs.validate.ValidateParamBetweenLong;
import org.sfs.validate.ValidateParamExists;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.parseLong;
import static java.net.HttpURLConnection.HTTP_NOT_MODIFIED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.sfs.rx.Defer.empty;
import static org.sfs.util.SfsHttpQueryParams.NODE;
import static org.sfs.util.SfsHttpQueryParams.POSITION;
import static org.sfs.util.SfsHttpQueryParams.VOLUME;

public class AckBlob implements Handler<SfsRequest> {

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
                .flatMap(httpServerRequest1 -> {
                    MultiMap headers = httpServerRequest1.params();
                    String volumeId = headers.get(VOLUME);
                    long position = parseLong(headers.get(POSITION));
                    final Volume volume = vertxContext.verticle().nodes().volumeManager().get(volumeId).get();
                    return volume.acknowledge(httpServerRequest1.vertxContext().vertx(), position);
                })
                .map(headerBlobOptional -> new Holder2<>(httpServerRequest, headerBlobOptional))
                .map(new WriteHeaderBlobAsHttpResponseHeaders<>())
                .single()
                .onErrorResumeNext(new HandleServerToBusy<>())
                .subscribe(new Terminus<Holder2<SfsRequest, Optional<HeaderBlob>>>(httpServerRequest) {

                    @Override
                    public void onNext(Holder2<SfsRequest, Optional<HeaderBlob>> holder) {
                        Optional<HeaderBlob> oBlob = holder.value1();
                        if (oBlob.isPresent()) {
                            holder.value0().response().setStatusCode(HTTP_NO_CONTENT);
                        } else {
                            holder.value0().response().setStatusCode(HTTP_NOT_MODIFIED);
                        }
                    }
                });

    }
}
